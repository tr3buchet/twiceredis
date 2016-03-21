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

import socket
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
    just returns None immediately if there are no messages. You'll want your
    handler to return message if you intend on using your own with get_message
    because it will return the result of your handler. By default it'll just
    log and return the message if you'd rather call your handler manually.

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
                    zelf._call_handler(message)
            except zelf.r.generic_error:
                LOG.exception('')

    def _default_handler(zelf, message):
        LOG.debug('processing: %s' % message)
        return message

    def _call_handler(zelf, message):
        try:
            return zelf.handler(message)
        except:
            LOG.exception('')
        finally:
            zelf.r.master.lrem(zelf._processing, -1, message)

    def get_message(zelf):
        """
        get one message if available else return None
        if message is available returns the result of handler(message)
        does not block!

        if you would like to call your handler manually, this is the way to
        go. don't pass in a handler to Listener() and the default handler will
        log and return the message for your own manual processing
        """
        try:
            message = zelf.r.master.rpoplpush(zelf.lijst, zelf._processing)
            if message:
                # NOTE(tr3buchet): got a message, process it
                LOG.debug('received: |%s|' % message)
                return zelf._call_handler(message)
        except zelf.r.generic_error:
            LOG.exception('')

    def listen(zelf):
        """
        listen indefinitely, handling messages as they come

        all redis specific exceptions are handled, anything your handler raises
        will not be handled. setting active to False on the Listener object
        will gracefully stop the listen() function
        """
        while zelf.active:
            try:
                msg = zelf.r.master.brpoplpush(zelf.lijst, zelf._processing,
                                               zelf.read_time)
                if msg:
                    # NOTE(tr3buchet): got a message, process it
                    LOG.debug('received: |%s|' % msg)
                    zelf._call_handler(msg)
            except zelf.r.generic_error:
                LOG.exception('')
            finally:
                time.sleep(0)


class TwiceRedis(object):
    """
    read and write sentinel connection pool backed redis client
    with disconnecting sentinel clients and redis clients

    examples
    # name give in redis/sentinel configs to the master
    master_name = 'master33'

    # list of sentinels to connect to, can just be one tuple if only one
    sentinels = [('host1', 26379), ('host2', 26379),...]

    password = 'dobbyd0llars'

    example pool and sentinel kwarg meanings
    # NOTE(tr3buchet): the sentinel and server connections can be configured
    #                  with different values. for example the sentinels do not
    #                  need to have tcp keepalive enabled because the sentinel
    #                  connections are so short lived. but you may if you wish
    # NOTE(tr3buchet): if you don't wish to pass any kwargs to either the
    #                  sentinel or server connection pools, pass in {}. if you
    #                  pass in None the defaults will be used

    # redis pings the master or slave before returning connecting from sentinel
    check_connection = True

    # number of other sentinels that must be up for the sentinel to return
    # a master or slave to you. useful to ensure a majority of sentinels are
    # running. raises MasterNotFound if there aren't N number of other
    # sentinels reachable. can be caught with TwiceRedis.generic_error.
    # if you have only 1 sentinel, this must be 0
    min_other_sentinels = 0

    # time for a blocking read on a socket to timeout, raising TimeoutError
    # catchable with TwiceRedis.generic_error
    socket_timeout = 15

    # timeout for establishing a socket connection, a ConnectionError will be
    # raised if timeout is reached. catchable with TwiceRedis.generic_error
    socket_connect_timeout = 15

    # enable tcp socket keepalive
    socket_keepalive = True

    # configure the specifics of tcp socket keepalive
    #     TCP_KEEPIDLE activates after (1 second) of idleness
    #     TCP_KEEPINTVL: keep alive send interval (3 seconds)
    #     TCP_KEEPCNT: close connection after (5) failed pings
    socket_keepalive_options = {socket.TCP_KEEPIDLE: 1,
                                socket.TCP_KEEPINTVL: 3,
                                socket.TCP_KEEPCNT: 5}

    # NOTE(tr3buchet): socket_timeout is used for both connect and send

    """
    generic_error = exceptions.RedisError
    _sko = {socket.TCP_KEEPIDLE: 1,
            socket.TCP_KEEPINTVL: 3,
            socket.TCP_KEEPCNT: 5}
    DEFAULT_POOL_KWARGS = {'check_connection': True,
                           'socket_timeout': 15,
                           'socket_connect_timeout': 5,
                           'socket_keepalive': True,
                           'socket_keepalive_options': _sko}
    DEFAULT_SENTINEL_KWARGS = {'min_other_sentinels': 0,
                               'socket_timeout': 5,
                               'socket_connect_timeout': 5}

    def __init__(selfie, master_name, sentinels, password=None,
                 pool_kwargs=None, sentinel_kwargs=None):
        if pool_kwargs is None:
            pool_kwargs = selfie.DEFAULT_POOL_KWARGS
        if sentinel_kwargs is None:
            sentinel_kwargs = selfie.DEFAULT_SENTINEL_KWARGS

        # NOTE(tr3buchet) always the first sentinel will be (re)used by the
        #                 connection pool unless it fails to provide a
        #                 good master or slaves during dicovery, in which case
        #                 the next in the list is tried. so we shuffle the list
        #                 here to distribute the sentinel load around
        sentinels = list(sentinels)
        random.shuffle(sentinels)

        cp = sentinel.SentinelConnectionPool
        sentinel_manager = DisconnectingSentinel(sentinels, **sentinel_kwargs)
        master_pool = cp(master_name, sentinel_manager, password=password,
                         is_master=True, **pool_kwargs)
        slave_pool = cp(master_name, sentinel_manager, password=password,
                        is_master=False, **pool_kwargs)

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
