# -*- coding: utf-8 -*-
#
# Copyright 2015 Trey Morris
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#


import redis
from redis import sentinel
import random


class DisconnectingSentinel(sentinel.Sentinel):
    """
    sentinel that disconnections after slave/master discovery
    """

    def disconnect_sentinels(selfie):
        for s in selfie.sentinels:
            s.connection_pool.disconnect()

    def discover_master(selfie, *args, **kwargs):
        ret = super(DisconnectingSentinel, selfie).discover_master(*args,
                                                                   **kwargs)
        selfie.disconnect_sentinels()
        return ret

    def discover_slaves(selfie, *args, **kwargs):
        ret = super(DisconnectingSentinel, selfie).discover_slaves(*args,
                                                                   **kwargs)
        selfie.disconnect_sentinels()
        return ret


class TwiceRedis(object):
    """
    read and write sentinel connection pool backed redis client
    """
    def __init__(selfie, master_name, sentinels, password=None,
                 check_connection=False, socket_timeout=None,
                 min_other_sentinels=0,
                 pool_kwargs=None, client_kwargs=None):

        pool_kwargs = {} if pool_kwargs is None else pool_kwargs
        client_kwargs = {} if client_kwargs is None else client_kwargs
        # NOTE(tr3buchet) always the first sentinel will be (re)used by the
        #                 connection pool unless it fails to provide a
        #                 good master or slaves during dicovery, in which case
        #                 the next in the list is tried. so we shuffle the list
        #                 here to distribute the sentinel load around
        random.shuffle(sentinels)

        cp = sentinel.SentinelConnectionPool
        master_pool = cp(master_name,
                         DisconnectingSentinel(sentinels, min_other_sentinels),
                         is_master=True, check_connection=check_connection,
                         password=password, **pool_kwargs)
        slave_pool = cp(master_name,
                        DisconnectingSentinel(sentinels, min_other_sentinels),
                        is_master=False, check_connection=check_connection,
                        password=password, **pool_kwargs)

        selfie.write_client = redis.StrictRedis(connection_pool=master_pool,
                                                socket_timeout=socket_timeout,
                                                **client_kwargs)
        selfie.read_client = redis.StrictRedis(connection_pool=slave_pool,
                                               socket_timeout=socket_timeout,
                                               **client_kwargs)

    @property
    def master(selfie):
        return selfie.write_client

    @property
    def slave(selfie):
        return selfie.read_client

    @property
    def write(selfie):
        return selfie.write_client

    @property
    def read(selfie):
        return selfie.read_client

    def disconnect(selfie):
        selfie.write_client.connection_pool.disconnect()
        selfie.read_client.connection_pool.disconnect()
