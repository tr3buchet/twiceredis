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
import time

import logging
LOG = logging.getLogger(__name__)


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


class Listener(object):
    """
    TwiceRedis based persistent message handler that can't lose messages

    reliably listen for messages lpushed to a list.
    when a message is received, in one transaction, the message
    will be moved to a processing list. only after the message
    is handled will it be removed from the processing list.

    ## read_time ##
    read_time is the duration we listen for a message before continuing to
    the next iteration of the loop. it is used in conjunction with redis's
    BRPOPLPUSH so Listener can loop slowly when there is little traffic
    while going as quickly as possible when there is much traffic. it also
    prevents timeout exceptions during the listen process. read_time should be
    set to something less than the socket_timeout/tcp keepalive failure time.
    listen will also generate a couple of packets every read_time interval
    even if there are no messages being sent, this keeps the socket alive
    and aids in the detection of a stale socket more quickly.

    if you would rather not mess with read_time, a listen thread, or blocking
    calls of any kind, use Lister.get_message(), it works like listen except it
    just returns None immediately if there are no messages.

    ## old messages ##
    Listener's init will process any old messages laying around in the
    processing list. if you have 2+ listeners listening to the same list, make
    sure message handling is idempotent or use a different processing_suffix
    for each listener. in general you want the processing_suffix to stay the
    same when restarting a worker using Listener objects or messages could
    be lost in the old processing list (ex don't generate new uuid each run)

    ## arguments ##
    twice_redis: instantiated twice_redis object
    lijst: string name of the list the listener will be watching
    handler: the function that handles the message (only takes msg as arg)
    read_time: duration to block while waiting for message
    processing_suffix: suffix added to lijst = list where messages are
                       stored while being processed
    """
    def __init__(zelf, twice_redis, lijst, handler=None, read_time=5,
                 processing_suffix='|processing'):
        zelf.r = twice_redis
        zelf.lijst = lijst
        zelf._processing = lijst + processing_suffix
        zelf.handler = handler or zelf._default_handler
        zelf.read_time = read_time
        zelf.active = True

        # NOTE(tr3buchet): clean up old unfinished business
        message = True
        while message:
            try:
                message = zelf.r.master.lindex(zelf._processing, -1)
                if message:
                    LOG.warn('found old message: |%s|' % message)
                    zelf._call_handler(lijst, message)
            except zelf.r.generic_error as e:
                LOG.exception(e)

    def _default_handler(zelf, message):
        LOG.debug('processing: %s' % message)
        return message

    def _call_handler(zelf, message):
        try:
            zelf.handler(message)
        except:
            LOG.exception()
        finally:
            zelf.r.master.lrem(zelf._processing, -1, message)

    def get_message(zelf):
        """
        get 1 message if available, else return None
        does not block!
        """
        try:
            message = zelf.r.master.rpoplpush(zelf.lijst, zelf._processing)
            if message:
                # NOTE(tr3buchet): got a message, process it
                LOG.debug('received: |%s|' % message)
                zelf._call_handler(message)
        except zelf.r.generic_error:
            LOG.exception()

    def listen(zelf):
        while zelf.active:
            try:
                msg = zelf.r.master.brpoplpush(zelf.lijst, zelf._processing,
                                               zelf.read_time)
                if msg:
                    # NOTE(tr3buchet): got a message, process it
                    LOG.debug('received: |%s|' % msg)
                    zelf._call_handler(msg)
            except zelf.r.generic_error:
                LOG.exception()
            finally:
                time.sleep(0)


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
        sentinel_manager = DisconnectingSentinel(sentinels,
                                                 min_other_sentinels,
                                                 socket_timeout=socket_timeout)
        master_pool = cp(master_name, sentinel_manager,
                         is_master=True, check_connection=check_connection,
                         password=password, socket_timeout=socket_timeout,
                         socket_keepalive=socket_keepalive,
                         socket_keepalive_options=socket_keepalive_options,
                         **pool_kwargs)
        slave_pool = cp(master_name, sentinel_manager,
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
