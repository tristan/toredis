import logging
import socket

from collections import deque
from functools import partial

import hiredis

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from tornado import stack_context

from toredis.commands import RedisCommandsMixin
from toredis.pipeline import Pipeline
from toredis._compat import string_types, text_type


logger = logging.getLogger(__name__)


class Client(RedisCommandsMixin):
    def __init__(self, io_loop=None, host='localhost', port=6379, usock=None, pool=None, callback=None):
        """
            Constructor
            :param io_loop:
                IOLoop instance to use, will be ignored if pool is present
            :param host:
                Host to connect to
            :param port:
                Port
            :param usock:
                The unix socket to connect to (host and port are ignored if this is present)
            :param pool:
                The connection pool to use
        """
        self._pool = pool
        if self._pool is not None:
            self._connection = pool.get_connection(callback)
        else:
            self._connection = Connection(host=host, port=port, usock=usock, io_loop=io_loop)
            self._connection.connect(callback)

    def close(self):
        if self._pool:
            self._pool.release_connection(self._connection)
            self._connection = None
        else:
            self._connection.close()
            self._connection = None

    def send_message(self, args, callback=None):
        self._connection.send_message(args, callback=callback)

    def send_messages(self, args_pipeline, callback=None):
        self._connection.send_messages(args_pipeline, callback=callback)

    def psubscribe(self, patterns, callback=None):
        if self._pool:
            raise Exception("Should not use PSUBSCRIBE using connections from a pool, create a dedicated connection instead")
        logger.debug("wat?")
        self._connection.set_sub_callback(callback)
        super(Client, self).psubscribe(patterns, callback=None)

    def subscribe(self, channels, callback=None):
        if self._pool:
            raise Exception("Should not use SUBSCRIBE using connections from a pool, create a dedicated connection instead")
        self._connection.set_sub_callback(callback)
        super(Client, self).subscribe(channels, callback=None)

    def pipeline(self):
        return Pipeline(self._connection)


class ConnectionPool(object):
    def __init__(self, io_loop=None, host='localhost', port=6379, usock=None):
        """
            Constructor
            :param host:
                Host to connect to
            :param port:
                Port
            :param usock:
                (Optional) The unix socket to connect to (host and port are ignored if this is present)
            :param io_loop:
                Optional IOLoop instance
        """
        self._io_loop = io_loop or IOLoop.instance()
        self._host = host
        self._port = port
        self._usock = usock

        self._connections = deque()

    def get_connection(self, callback=None):
        # TODO: support multiple ioloops?
        # TODO: limit the number of connections possible
        try:
            connection = self._connections.popleft()
            if not connection.is_connected():
                connection.connect(callback)
            elif callback is not None:
                self._io_loop.add_callback(callback)
            return connection
        except IndexError:
            return self.make_connection(callback)

    def make_connection(self, callback=None):
        connection = Connection(host=self._host, port=self._port,
                                usock=self._usock, io_loop=self._io_loop)
        connection.connect(callback)
        return connection

    def _release(self, connection):
        # TODO: figure out what else we should to do here, probably should call connection._on_close
        self._conncetions.append(connection)


    def release_connection(self, connection):
        if not isinstance(connection, Connection):
            raise Exception("Trying to release non-connection")
        connection.when_idle(self.release_connection, connection)


class Connection(object):
    """
        Redis connection class
    """
    def __init__(self, host='localhost', port=6379, usock=None, io_loop=None):
        """
            Constructor
            :param host:
                Host to connect to
            :param port:
                Port
            :param usock:
                (Optional) The unix socket to connect to (host and port are ignored if this is present)
            :param io_loop:
                Optional IOLoop instance
        """
        self._io_loop = io_loop or IOLoop.instance()

        self._addr = usock if usock else (host, port)
        self._prot = socket.AF_UNIX if usock else socket.AF_INET

        self._stream = None

        self.reader = None
        self.callbacks = deque()

        self._sub_callback = False

        # function to call when the connection becomes idle
        self._when_idle = None

    def connect(self, callback=None):
        """
            Connect to redis server

            :param callback:
                (Optional) callback to be triggered upon connection
        """
        sock = socket.socket(self._prot, socket.SOCK_STREAM, 0)
        self._connect(sock, self._addr, callback)

    def on_disconnect(self):
        """
            Override this method if you want to handle disconnections
        """
        pass

    # State
    def is_idle(self):
        """
            Check if client is not waiting for any responses
        """
        return len(self.callbacks) == 0

    def is_connected(self):
        """
            Check if client is still connected
        """
        return bool(self._stream) and not self._stream.closed()

    def send_message(self, args, callback=None):
        """
            Send command to redis

            :param args:
                Arguments to send
            :param callback:
                Callback
        """
        # Special case for pub-sub
        cmd = args[0]

        if (self._sub_callback is not None and
            cmd not in ('PSUBSCRIBE', 'SUBSCRIBE', 'PUNSUBSCRIBE', 'UNSUBSCRIBE')):
            raise ValueError('Cannot run normal command over PUBSUB connection')

        # Send command
        self._stream.write(self.format_message(args))
        if callback is not None:
            callback = stack_context.wrap(callback)
        self.callbacks.append((callback, None))

    def send_messages(self, args_pipeline, callback=None):
        """
            Send command pipeline to redis

            :param args_pipeline:
                Arguments pipeline to send
            :param callback:
                Callback
        """

        if self._sub_callback is not None:
            raise ValueError('Cannot run pipeline over PUBSUB connection')

        # Send command pipeline
        messages = [self.format_message(args) for args in args_pipeline]
        self._stream.write(b"".join(messages))
        if callback is not None:
            callback = stack_context.wrap(callback)
        self.callbacks.append((callback, (len(messages), [])))

    def format_message(self, args):
        """
            Create redis message

            :param args:
                Message data
        """
        l = "*%d" % len(args)
        lines = [l.encode('utf-8')]
        for arg in args:
            if not isinstance(arg, string_types):
                arg = str(arg)
            if isinstance(arg, text_type):
                arg = arg.encode('utf-8')
            l = "$%d" % len(arg)
            lines.append(l.encode('utf-8'))
            lines.append(arg)
        lines.append(b"")
        return b"\r\n".join(lines)

    def close(self):
        """
            Close redis connection
        """
        self.quit()
        self._stream.close()

    # Pub/sub commands
    def set_sub_callback(self, callback):
        if self._sub_callback is None:
            self._sub_callback = callback

        assert self._sub_callback == callback

    # Helpers
    def _connect(self, sock, addr, callback):
        self._reset()

        self._stream = IOStream(sock, io_loop=self._io_loop)
        self._stream.connect(addr, callback=callback)
        self._stream.read_until_close(self._on_close, self._on_read)

    # Event handlers
    def _on_read(self, data):
        self.reader.feed(data)

        resp = self.reader.gets()

        while resp is not False:
            if self._sub_callback:
                try:
                    self._sub_callback(resp)
                except:
                    logger.exception('SUB callback failed')
            else:
                if self.callbacks:
                    callback, callback_data = self.callbacks[0]
                    if callback_data is None:
                        callback_resp = resp
                    else:
                        # handle pipeline responses
                        num_resp, callback_resp = callback_data
                        callback_resp.append(resp)
                        while len(callback_resp) < num_resp:
                            resp = self.reader.gets()
                            if resp is False:
                                # callback_resp is yet incomplete
                                return
                            callback_resp.append(resp)
                    self.callbacks.popleft()
                    if callback is not None:
                        try:
                            callback(callback_resp)
                        except:
                            logger.exception('Callback failed')
                else:
                    logger.debug('Ignored response: %s' % repr(resp))

            resp = self.reader.gets()
        if self._when_idle and len(self.callbacks) == 0:
            self._when_idle()
            self._when_idle = None

    def _on_close(self, data=None):
        if data is not None:
            self._on_read(data)

        # Trigger any pending callbacks
        callbacks = self.callbacks
        self.callbacks = deque()

        if callbacks:
            for cb in callbacks:
                callback, callback_data = cb
                if callback is not None:
                    try:
                        callback(None)
                    except:
                        logger.exception('Exception in callback')

        if self._sub_callback is not None:
            try:
                self._sub_callback(None)
            except:
                logger.exception('Exception in SUB callback')
            self._sub_callback = None

        # Trigger on_disconnect
        self.on_disconnect()

    def _reset(self):
        self.reader = hiredis.Reader()
        self._sub_callback = None

    def when_idle(self, fn, *args, **kwargs):
        if self.is_idle():
            fn(*args, **kwargs)
            return
        self._when_idle = partial(fn, *args, **kwargs)
