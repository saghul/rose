
import collections
import concurrent.futures
import errno
import logging
import pyuv
import socket
import sys

try:
    import ssl
except ImportError:
    ssl = None
try:
    import signal
except ImportError:
    signal = None

from tulip import events
from tulip import futures
from tulip import selector_events
from tulip import tasks


# Errno values indicating the socket isn't ready for I/O just yet.
_TRYAGAIN = frozenset((errno.EAGAIN, errno.EWOULDBLOCK, errno.EINPROGRESS))
if sys.platform == 'win32':
    _TRYAGAIN = frozenset(list(_TRYAGAIN) + [errno.WSAEWOULDBLOCK])

# Argument for default thread pool executor creation.
_MAX_WORKERS = 5

def _raise_stop_error():
    raise _StopError


class EventLoop(events.AbstractEventLoop):

    @staticmethod
    def SocketTransport(event_loop, sock, protocol, waiter=None):
        return selector_events._SelectorSocketTransport(event_loop, sock, protocol, waiter)

    @staticmethod
    def SslTransport(event_loop, rawsock, protocol, sslcontext, waiter):
        return selector_events._SelectorSslTransport(event_loop, rawsock, protocol, sslcontext, waiter)

    def __init__(self):
        super().__init__()
        self._loop = pyuv.Loop()
        self._stop = False
        self._default_executor = None

        self._fd_map = {}
        self._signal_handlers = {}
        self._ready = collections.deque()
        self._timers = collections.deque()

        self._waker = pyuv.Async(self._loop, lambda h: None)
        self._waker.unref()

        self._ticker = pyuv.Idle(self._loop)
        self._ticker.unref()

        self._stop_h = pyuv.Idle(self._loop)
        self._stop_h.unref()

        self._ready_processor = pyuv.Check(self._loop)
        self._ready_processor.start(self._process_ready)
        self._ready_processor.unref()

    def run(self):
        self._stop = False
        while not self._stop and self._run_once():
            pass

    def run_forever(self):
        handler = self.call_repeatedly(24*3600, lambda: None)
        try:
            self.run()
        finally:
            handler.cancel()

    def run_once(self, timeout=None):
        if timeout is not None:
            timer = pyuv.Timer(self._loop)
            timer.start(lambda x: None, timeout, 0)
        self._run_once()
        if timeout is not None:
            timer.close()

    def run_until_complete(self, future, timeout=None):
        if timeout is None:
            timeout = 0x7fffffff/1000.0  # 24 days
        future.add_done_callback(lambda _: self.stop())
        handler = self.call_later(timeout, _raise_stop_error)
        self.run()
        handler.cancel()
        if future.done():
            return future.result()  # May raise future.exception().
        else:
            raise futures.TimeoutError

    def stop(self):
        self._stop = True
        if not self._stop_h.active:
            self._stop_h.start(lambda h: h.stop())

    def close(self):
        self._fd_map.clear()
        self._signal_handlers.clear()
        self._ready.clear()
        self._timers.clear()

        self._waker.close()
        self._ticker.close()
        self._stop_h.close()
        self._ready_processor.close()

        def cb(handle):
            if not handle.closed:
                handle.close()
        self._loop.walk(cb)
        # Run a loop iteration so that close callbacks are called and resources are freed
        assert not self._loop.run(pyuv.UV_RUN_NOWAIT)
        self._loop = None

    # Methods returning Handlers for scheduling callbacks.

    def call_later(self, delay, callback, *args):
        if delay <= 0:
            return self.call_soon(callback, *args)
        handler = events.make_handler(callback, args)
        timer = pyuv.Timer(self._loop)
        timer.handler = handler
        timer.start(self._timer_cb, delay, 0)
        self._timers.append(timer)
        return handler

    def call_repeatedly(self, interval, callback, *args):  # NEW!
        if interval <= 0:
            raise ValueError('invalid interval specified: {}'.format(interval))
        handler = events.make_handler(callback, args)
        timer = pyuv.Timer(self._loop)
        timer.handler = handler
        timer.start(self._timer_cb, interval, interval)
        self._timers.append(timer)
        return handler

    def call_soon(self, callback, *args):
        handler = events.make_handler(callback, args)
        self._ready.append(handler)
        return handler

    def call_soon_threadsafe(self, callback, *args):
        handler = self.call_soon(callback, *args)
        self._waker.send()
        return handler

    # Methods returning Futures for interacting with threads.

    def wrap_future(self, future):
        if isinstance(future, futures.Future):
            return future  # Don't wrap our own type of Future.
        new_future = futures.Future()
        future.add_done_callback(lambda f: self.call_soon_threadsafe(new_future._copy_state, f))
        return new_future

    def run_in_executor(self, executor, callback, *args):
        if isinstance(callback, events.Handler):
            assert not args
            assert not isinstance(callback, events.Timer)
            if callback.cancelled:
                f = futures.Future()
                f.set_result(None)
                return f
            callback, args = callback.callback, callback.args
        if executor is None:
            executor = self._default_executor
            if executor is None:
                executor = concurrent.futures.ThreadPoolExecutor(_MAX_WORKERS)
                self._default_executor = executor
        return self.wrap_future(executor.submit(callback, *args))

    def set_default_executor(self, executor):
        self._default_executor = executor

    # Network I/O methods returning Futures.

    def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        return self.run_in_executor(None, socket.getaddrinfo,
                                    host, port, family, type, proto, flags)

    def getnameinfo(self, sockaddr, flags=0):
        return self.run_in_executor(None, socket.getnameinfo, sockaddr, flags)

    @tasks.task
    def create_connection(self, protocol_factory, host=None, port=None, *, ssl=False,
                          family=0, proto=0, flags=0, sock=None):
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host, port and sock can not be specified at the same time")

            infos = yield from self.getaddrinfo(
                host, port, family=family,
                type=socket.SOCK_STREAM, proto=proto, flags=flags)

            if not infos:
                raise socket.error('getaddrinfo() returned empty list')

            exceptions = []
            for family, type, proto, cname, address in infos:
                sock = None
                try:
                    sock = socket.socket(family=family, type=type, proto=proto)
                    sock.setblocking(False)
                    yield self.sock_connect(sock, address)
                except socket.error as exc:
                    if sock is not None:
                        sock.close()
                    exceptions.append(exc)
                else:
                    break
            else:
                if len(exceptions) == 1:
                    raise exceptions[0]
                else:
                    # If they all have the same str(), raise one.
                    model = str(exceptions[0])
                    if all(str(exc) == model for exc in exceptions):
                        raise exceptions[0]
                    # Raise a combined exception so the user can see all
                    # the various error messages.
                    raise socket.error('Multiple exceptions: {}'.format(
                        ', '.join(str(exc) for exc in exceptions)))
        elif sock is None:
            raise ValueError(
                "host and port was not specified and no sock specified")

        protocol = protocol_factory()
        waiter = futures.Future()
        if ssl:
            sslcontext = None if isinstance(ssl, bool) else ssl
            transport = self.SslTransport(self, sock, protocol, sslcontext, waiter)
        else:
            transport = self.SocketTransport(self, sock, protocol, waiter)

        yield from waiter
        return transport, protocol

    @tasks.task
    def start_serving(self, protocol_factory, host=None, port=None, *,
                      family=0, proto=0, flags=0, backlog=100, sock=None):
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host, port and sock can not be specified at the same time")

            infos = yield from self.getaddrinfo(
                host, port, family=family,
                type=socket.SOCK_STREAM, proto=proto, flags=flags)

            if not infos:
                raise socket.error('getaddrinfo() returned empty list')

            # TODO: Maybe we want to bind every address in the list
            # instead of the first one that works?
            exceptions = []
            for family, type, proto, cname, address in infos:
                sock = socket.socket(family=family, type=type, proto=proto)
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.bind(address)
                except socket.error as exc:
                    sock.close()
                    exceptions.append(exc)
                else:
                    break
            else:
                raise exceptions[0]
        elif sock is None:
            raise ValueError("host and port was not specified and no sock specified")

        sock.listen(backlog)
        sock.setblocking(False)
        self.add_reader(sock.fileno(), self._accept_connection, protocol_factory, sock)
        return sock

    def _accept_connection(self, protocol_factory, sock):
        try:
            conn, addr = sock.accept()
        except socket.error as exc:
            if exc.errno in _TRYAGAIN:
                return  # False alarm.
            # Bad error.  Stop serving.
            self.remove_reader(sock.fileno())
            sock.close()
            # There's nowhere to send the error, so just log it.
            # TODO: Someone will want an error handler for this.
            logging.exception('Accept failed')
            return
        protocol = protocol_factory()
        transport = self.SocketTransport(self, conn, protocol)
        # It's now up to the protocol to handle the connection.

    # Level-trigered I/O methods.
    # The add_*() methods return a Handler.
    # The remove_*() methods return True if something was removed,
    # False if there was nothing to delete.

    def add_reader(self, fd, callback, *args):
        handler = events.make_handler(callback, args)
        try:
            poll_h = self._fd_map[fd]
        except KeyError:
            poll_h = self._create_poll_handle(fd)
            self._fd_map[fd] = poll_h
        else:
            poll_h.stop()

        poll_h.pevents |= pyuv.UV_READABLE
        poll_h.read_handler = handler
        poll_h.start(poll_h.pevents, self._poll_cb)

        return handler

    def remove_reader(self, fd):
        try:
            poll_h = self._fd_map[fd]
        except KeyError:
            return False
        else:
            poll_h.stop()
            poll_h.pevents &= ~pyuv.UV_READABLE
            poll_h.read_handler = None
            if poll_h.pevents == 0:
                del self._fd_map[fd]
                poll_h.close()
            else:
                poll_h.start(poll_h.pevents, self._poll_cb)
            return True

    def add_writer(self, fd, callback, *args):
        handler = events.make_handler(callback, args)
        try:
            poll_h = self._fd_map[fd]
        except KeyError:
            poll_h = self._create_poll_handle(fd)
            self._fd_map[fd] = poll_h
        else:
            poll_h.stop()

        poll_h.pevents |= pyuv.UV_WRITABLE
        poll_h.write_handler = handler
        poll_h.start(poll_h.pevents, self._poll_cb)

        return handler

    def remove_writer(self, fd):
        try:
            poll_h = self._fd_map[fd]
        except KeyError:
            return False
        else:
            poll_h.stop()
            poll_h.pevents &= ~pyuv.UV_WRITABLE
            poll_h.write_handler = None
            if poll_h.pevents == 0:
                del self._fd_map[fd]
                poll_h.close()
            else:
                poll_h.start(poll_h.pevents, self._poll_cb)
            return True

    add_connector = add_writer

    remove_connector = remove_writer

    # Completion based I/O methods returning Futures.

    def sock_recv(self, sock, n):
        fut = futures.Future()
        self._sock_recv(fut, False, sock, n)
        return fut

    def _sock_recv(self, fut, registered, sock, n):
        fd = sock.fileno()
        if registered:
            # Remove the callback early.  It should be rare that the
            # selector says the fd is ready but the call still returns
            # EAGAIN, and I am willing to take a hit in that case in
            # order to simplify the common case.
            self.remove_reader(fd)
        if fut.cancelled():
            return
        try:
            data = sock.recv(n)
            fut.set_result(data)
        except socket.error as exc:
            if exc.errno not in _TRYAGAIN:
                fut.set_exception(exc)
            else:
                self.add_reader(fd, self._sock_recv, fut, True, sock, n)

    def sock_sendall(self, sock, data):
        fut = futures.Future()
        self._sock_sendall(fut, False, sock, data)
        return fut

    def _sock_sendall(self, fut, registered, sock, data):
        fd = sock.fileno()
        if registered:
            self.remove_writer(fd)
        if fut.cancelled():
            return
        n = 0
        try:
            if data:
                n = sock.send(data)
        except socket.error as exc:
            if exc.errno not in _TRYAGAIN:
                fut.set_exception(exc)
                return
        if n == len(data):
            fut.set_result(None)
        else:
            if n:
                data = data[n:]
            self.add_writer(fd, self._sock_sendall, fut, True, sock, data)

    def sock_connect(self, sock, address):
        # That address better not require a lookup!  We're not calling
        # self.getaddrinfo() for you here.  But verifying this is
        # complicated; the socket module doesn't have a pattern for
        # IPv6 addresses (there are too many forms, apparently).
        fut = futures.Future()
        self._sock_connect(fut, False, sock, address)
        return fut

    def _sock_connect(self, fut, registered, sock, address):
        fd = sock.fileno()
        if registered:
            self.remove_connector(fd)
        if fut.cancelled():
            return
        try:
            if not registered:
                # First time around.
                sock.connect(address)
            else:
                err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if err != 0:
                    # Jump to the except clause below.
                    raise socket.error(err, 'Connect call failed')
            fut.set_result(None)
        except socket.error as exc:
            if exc.errno not in _TRYAGAIN:
                fut.set_exception(exc)
            else:
                self.add_connector(fd, self._sock_connect, fut, True, sock, address)

    def sock_accept(self, sock):
        fut = futures.Future()
        self._sock_accept(fut, False, sock)
        return fut

    def _sock_accept(self, fut, registered, sock):
        fd = sock.fileno()
        if registered:
            self.remove_reader(fd)
        if fut.cancelled():
            return
        try:
            conn, address = sock.accept()
            conn.setblocking(False)
            fut.set_result((conn, address))
        except socket.error as exc:
            if exc.errno not in _TRYAGAIN:
                fut.set_exception(exc)
            else:
                self.add_reader(fd, self._sock_accept, fut, True, sock)

    # Signal handling.

    def add_signal_handler(self, sig, callback, *args):
        self._validate_signal(sig)
        signal_h = pyuv.Signal(self._loop)
        handler = events.make_handler(callback, args)
        signal_h.handler = handler
        try:
            signal_h.start(self._signal_cb, sig)
        except Exception as e:
            signal_h.close()
            raise RuntimeError(str(e))
        else:
            self._signal_handlers[sig] = signal_h
        return handler

    def remove_signal_handler(self, sig):
        self._validate_signal(sig)
        try:
            signal_h = self._signal_handlers.pop(sig)
        except KeyError:
            return False
        del signal_h.handler
        signal_h.close()
        return True

    # Private / internal methods

    def _run_once(self):
        # Check if there are cancelled timers, if so close the handles
        self._check_timers()

        # If there is something ready to be run, prevent the loop from blocking for i/o
        if self._ready:
            self._ticker.ref()
            self._ticker.start(lambda x: None)

        return self._loop.run(pyuv.UV_RUN_ONCE) or bool(self._ready)

    def _check_timers(self):
        for timer in [timer for timer in self._timers if timer.handler.cancelled]:
            del timer.handler
            timer.close()
            self._timers.remove(timer)

    def _timer_cb(self, timer):
        if timer.handler.cancelled:
            del timer.handler
            self._timers.remove(timer)
            timer.close()
            return
        self._ready.append(timer.handler)
        if not timer.repeat:
            del timer.handler
            self._timers.remove(timer)
            timer.close()

    def _signal_cb(self, signal_h, signum):
        if signal_h.handler.cancelled:
            self.remove_signal_handler(signum)
            return
        self._ready.append(signal_h.handler)

    def _poll_cb(self, poll_h, events, error):
        if error is not None:
            # An error happened, signal both readability and writability and
            # let the error propagate
            if poll_h.read_handler is not None:
                if poll_h.read_handler.cancelled:
                    self.remove_reader(poll_h.fd)
                else:
                    self._ready.append(poll_h.read_handler)
            if poll_h.write_handler is not None:
                if poll_h.write_handler.cancelled:
                    self.remove_writer(poll_h.fd)
                else:
                    self._ready.append(poll_h.write_handler)
            return

        old_events = poll_h.pevents
        modified = False

        if events & pyuv.UV_READABLE:
            if poll_h.read_handler is not None:
                if poll_h.read_handler.cancelled:
                    self.remove_reader(poll_h.fd)
                    modified = True
                else:
                    self._ready.append(poll_h.read_handler)
            else:
                poll_h.pevents &= ~pyuv.UV_READABLE
        if events & pyuv.UV_WRITABLE:
            if poll_h.write_handler is not None:
                if poll_h.write_handler.cancelled:
                    self.remove_writer(poll_h.fd)
                    modified = True
                else:
                    self._ready.append(poll_h.write_handler)
            else:
                poll_h.pevents &= ~pyuv.UV_WRITABLE

        if not modified and old_events != poll_h.pevents:
            # Rearm the handle
            poll_h.stop()
            poll_h.start(poll_h.pevents, self._poll_cb)

    def _process_ready(self, handle):
        # Stop the ticker in case it was active
        if self._ticker.active:
            self._ticker.stop()
            self._ticker.unref()
        # This is the only place where callbacks are actually *called*.
        # All other places just add them to ready.
        # Note: We run all currently scheduled callbacks, but not any
        # callbacks scheduled by callbacks run this time around --
        # they will be run the next time (after another I/O poll).
        # Use an idiom that is threadsafe without using locks.
        ntodo = len(self._ready)
        for i in range(ntodo):
            handler = self._ready.popleft()
            if not handler.cancelled:
                try:
                    handler.callback(*handler.args)
                except Exception:
                    logging.exception('Exception in callback %s %r', handler.callback, handler.args)

    def _create_poll_handle(self, fdobj):
        fd = self._fileobj_to_fd(fdobj)
        poll_h = pyuv.Poll(self._loop, fd)
        poll_h.fd = fd
        poll_h.pevents = 0
        poll_h.read_handler = None
        poll_h.write_handler = None
        return poll_h

    def _fileobj_to_fd(self, fileobj):
        """Return a file descriptor from a file object.

        Parameters:
        fileobj -- file descriptor, or any object with a `fileno()` method

        Returns:
        corresponding file descriptor
        """
        if isinstance(fileobj, int):
            fd = fileobj
        else:
            try:
                fd = int(fileobj.fileno())
            except (ValueError, TypeError):
                raise ValueError("Invalid file object: {!r}".format(fileobj))
        return fd

    def _validate_signal(self, sig):
        """Internal helper to validate a signal.

        Raise ValueError if the signal number is invalid or uncatchable.
        Raise RuntimeError if there is a problem setting up the handler.
        """
        if not isinstance(sig, int):
            raise TypeError('sig must be an int, not {!r}'.format(sig))
        if signal is None:
            raise RuntimeError('Signals are not supported')
        if not (1 <= sig < signal.NSIG):
            raise ValueError('sig {} out of range(1, {})'.format(sig, signal.NSIG))
        if sys.platform == 'win32':
            raise RuntimeError('Signals are not really supported on Windows')

    def _socketpair(self):
        # TODO: remove this, it's just here for the test suite
        return socket.socketpair()

