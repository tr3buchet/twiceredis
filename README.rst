==========
TwiceRedis
==========
read and write sentinel connection pool backed redis client
with disconnecting sentinel clients and redis clients

when using a redis connection backed by a ``SentinelConnectionPool``,
the pool is initialized to connect to either the master or slaves.
this is annoying if you want to read from slaves and occasionally
write to the master.
``TwiceRedis`` solves this by having two clients,
one for master and one for slaves:

* ``tr.master`` also aliased as ``tr.write``
* ``tr.slave`` also aliased as ``tr.read``

the clients are each backed by a separate ``SentinelConnectionPool``
initialized to connect to the master or slaves respectively

``TwiceRedis`` also uses a ``DisconnectingSentinel`` class to drastically
reduce the number of active connections to the redis sentinel service(s).
this class drops connection to the chosen sentinel once the master or
slave has been chosen

the ``DisconnectionSentinel`` class also filters slaves a little more
intelligently than the base ``Sentinel`` class does. In addition to
insuring slaves are not ``sdown`` or ``odown`` it makes sure the slaves
``master-link-status`` is 'ok'

``TwiceRedis`` randomizes the sentinel list so each ``TwiceRedis``
object will be connecting to a random sentinel in the list instead of
them all connecting to the first one (for as long as it works).
this shuffling is probably a bit superfluous used in conjunction with
``DisconnectingSentinel``, but at worst will reduce the load on the
first sentinel in the ``sentinels`` list

``TwiceRedis`` also utilizes a subclass of ``StrictRedis`` called
``DisconnectRedis`` that adds a ``disconnect()`` function to all the clients
making it easier to manage individual connections to the redis services


~~~~~
usage
~~~~~
.. code:: python

    from twiceredis import TwiceRedis
    sentinels = [('10.10.10.10', 26379),
                 ('10.10.10.11', 26379),
                 ('10.10.10.12', 26379)]
    tr = TwiceRedis('master01', sentinels, 'tötes_passowrd')
    x = tr.slave.get('superkey')
    tr.master.set('je mange', 'huehue')
    x = tr.read.get('nous mangeons')
    tr.write.del('superkey')

pipelines work great too, you just have decide whether you need to write
during one or not, if write is needed, use ``tr.master``, else use ``tr.slave``

.. code:: python

    with tr.master.pipeline() as wpipe:
        wpipe.set('turle', 'power')
        wpipe.set('tr3buchet', 'tötes')
        wpipe.execute()

to connect, get a key, and then disconnect to reduce active connections

.. code:: python

    x = tr.slave.get('some key')
    tr.slave.disconnect()

and afterward reconnection will happen seamlessly as needed \o/
and chances are you'll hit a different slave

.. code:: python

    x = tr.slave.get('some other key')

or if you want to disconnect both ``tr.master`` and ``tr.slave``,
``tr.disconnect()`` can be used. it calls ``disconnect()`` on both
the slave and master clients:

.. code:: python

    x = tr.slave.get('some key')
    tr.master.publish('topic', x)
    tr.disconnect()

    # ... and later on reconnect seamlessly
    tr.master.set('some key', 'le totes!')
    x = tr.slave.get('some_key')

as far as tuning goes, check out docstring in the definition of ``TwiceRedis`` in `the source <https://github.com/tr3buchet/twiceredis/blob/master/twiceredis/client.py>`_. the default arguments to ``pool_kwargs`` and ``sentinel_kwargs`` are defined to make it easy to alter the specific parameters to your needs. it's mostly timeouts and tcp keepalive stuff, but every environment is different, so the defaults which work in mine may not work in yours. here's an example of tweaking:

.. code:: python

    from twiceredis import TwiceRedis
    sentinels = [('10.10.10.10', 26379),
                 ('10.10.10.11', 26379),
                 ('10.10.10.12', 26379)]
    pool_kwargs = TwiceRedis.DEFAULT_POOL_KWARGS
    pool_kwargs['tcp_keepalive'] = False
    sentinel_kwargs = TwiceRedis.DEFAULT_SENTINEL_KWARGS
    sentinel_kwargs['min_other_sentinels'] = 2
    tr = TwiceRedis('master01', sentinels, 'tötes_passowrd',
                    pool_kwargs=pool_kwargs, sentinel_kwargs=sentinel_kwargs)

========
Listener
========
TwiceRedis based crazy durable message listener with persistent messages and in flight messages stored

created because I was trying to use redis pubsub but was being disconnected by firewalls and losing messages. trying to handle whether messages are subscribed to from the publishing side was really painful and full of fail. ``Listener`` allows you to reliably listen for messages lpushed to any list. when a message is received, and in one transaction only, the message will be moved to a processing list. only after the message is handled will it be removed from the processing list. ``Listener`` can listen indefinitely and handle any and all connection failures or master failovers that might happen, passive firewall drops be damned.


~~~~~
usage
~~~~~
.. code:: python

    # on the listener side of things
    from twiceredis import TwiceRedis
    from twiceredis import Listener
    sentinels = [('10.10.10.10', 26379),
                 ('10.10.10.11', 26379),
                 ('10.10.10.12', 26379)]
    tr = TwiceRedis('master01', sentinels, 'tötes_passowrd')
    l = Listener(tr, 'message_list')
    l.listen()      # <--- blocks and logs all messages that comes through

    # on the publisher side of things
    redis_client.lpush('message_list', 'incredibly important message')

and that's it. it's easy to use. if you need a little more customization try using your own handler. I recommend always returning ``message`` from the handler to it works well with ``get_message()`` which returns the result of the handler whether it is your custom handler or the built in default handler which logs the ``message`` and then returns it.

.. code:: python

    # again on the listener side of things
    def f(msg):
        do_thing(msg)
        print msg
        return msg

    l = Listener(tr, 'message_list', handler=f)
    l.listen()      # <--- blocks and calls f(msg) for each msg that comes through

if blocking isn't your thing, that's cool too, check out ``get_message()``. this example using the default handler will log the ``message`` and then return it for you to use however you wish. like the previous example, you may define your own handler and ignore or do whatever with the result of ``get_message()``

.. code:: python

    # always with the listener side of things
    l = Listener(tr, 'message_list')
    while some_loop_construct_is_true:
        msg = l.get_message()     # <--- does not block, returns None immediately if there is no message
        # do whatever with msg

you can manually handle messages with ``get_message()`` as well. the default handler is still called but it only logs and returns the message so you can handle it however you wish.

.. code:: python

    # always with the listener side of things
    l = Listener(tr, 'message_list')
    while some_loop_construct_is_true:
        some_handler(l.get_message())
        # do other things in your loop

``read_time`` is how long the ``listen()`` function will block per iteration when it hasn't received a message. it really doesn't matter what this value is as long as it is lower than the ``socket_timeout`` configured for the ``TwiceRedis`` object you pass to ``Listener`` on instantiation. if it is greater than ``socket_timeout`` there will be an exception raised each iteration, which is handled, but it's inefficient. I decided to implement a ``read_time`` pseudo timeout so the standard loop doesn't need to raise exceptions and it prevents getting stuck in a never ending listen if the socket hangs for whatever reason. NOTE!! ``read_time`` has nothing to do with the rate messages are handled. the loop will iterate as quickly as possible while it is receiving messages.

``processing_suffix`` is added to the event list name to build the list key that is used to store the in flight messages until handling is finished and can be changed to any string you like.

as far as exceptions or redis connection handling goes, if you start a ``listen()`` you can kill redis or do whatever, it can be down for a week, but as soon as it comes back up, ``Listener`` will pick up right where it left off as if nothing happened. each iteration will reuse or attempt to create a connection which relies on the sentinel backed nature of ``TwiceRedis`` to reconnect to the proper master (even if it changes due to failover or maintenance etc). it's built to be crazy durable.


=======
install
=======
``pip install twiceredis`` or clone the repo and ``python setup.py install``
