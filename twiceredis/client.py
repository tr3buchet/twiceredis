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
from redis import exceptions
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

    def filter_slaves(selfie, slaves):
        """
        Remove slaves that are in an ODOWN or SDOWN state
        also remove slaves that do not have 'ok' master-link-status
        """
        return [(s['ip'], s['port']) for s in slaves
                if not s['is_odown'] and
                not s['is_sdown'] and
                s['master-link-status'] == 'ok']


class DisconnectRedis(redis.StrictRedis):
    """
    normal redis.StrictRedis class + disconnect() function
    """
    def disconnect(selfie):
        selfie.connection_pool.disconnect()


class TwiceRedis(object):
    """
    read and write sentinel connection pool backed redis client
    with disconnecting sentinel clients and redis clients

    examples
    master_name = 'master33'
    sentinels = [('host1', 26379), ('host2', 26379),...]
    password = 'dobbyd0llars'
    check_connection = True
    min_other_sentinels = 2
    socket_timeout = 5
    socket_keepalive = True
    socket_keepalive_options = {socket.TCP_KEEPIDLE: 1,
                                socket.TCP_KEEPINTVL: 3,
                                socket.TCP_KEEPCNT: 5}
    pool_kwargs = additional connection options

    # NOTE(tr3buchet): socket keepalive options:
                       TCP_KEEPIDLE activates after (1 second) of idleness
                       TCP_KEEPINTVL: keep alive send interval (3 seconds)
                       TCP_KEEPCNT: close connection after (5) failed pings
    # NOTE(tr3buchet): socket_timeout is used for both connect and send

    """
    generic_error = exceptions.RedisError

    def __init__(selfie, master_name, sentinels, password=None,
                 check_connection=False, min_other_sentinels=0,
                 socket_timeout=None, socket_keepalive=False,
                 socket_keepalive_options=None,
                 pool_kwargs=None):

        pool_kwargs = {} if pool_kwargs is None else pool_kwargs

        # NOTE(tr3buchet) always the first sentinel will be (re)used by the
        #                 connection pool unless it fails to provide a
        #                 good master or slaves during dicovery, in which case
        #                 the next in the list is tried. so we shuffle the list
        #                 here to distribute the sentinel load around
        sentinels = list(sentinels)
        random.shuffle(sentinels)

        cp = sentinel.SentinelConnectionPool
        master_pool = cp(master_name,
                         DisconnectingSentinel(sentinels, min_other_sentinels),
                         is_master=True, check_connection=check_connection,
                         password=password, socket_timeout=socket_timeout,
                         socket_keepalive=socket_keepalive,
                         socket_keepalive_options=socket_keepalive_options,
                         **pool_kwargs)
        slave_pool = cp(master_name,
                        DisconnectingSentinel(sentinels, min_other_sentinels),
                        is_master=False, check_connection=check_connection,
                        password=password, socket_timeout=socket_timeout,
                        socket_keepalive=socket_keepalive,
                        socket_keepalive_options=socket_keepalive_options,
                        **pool_kwargs)

        selfie.write_client = DisconnectRedis(connection_pool=master_pool)
        selfie.read_client = DisconnectRedis(connection_pool=slave_pool)

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
        selfie.write_client.disconnect()
        selfie.read_client.disconnect()
