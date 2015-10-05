
import collections
import os
import pyuv
import socket
import sys
import time

try:
    import ssl
except ImportError:
    ssl = None

try:
    import signal
except ImportError:
    signal = None

import asyncio
from asyncio import base_events
from asyncio import selector_events
from asyncio.log import logger


# Argument for default thread pool executor creation.
_MAX_WORKERS = 5


class TimerHandle(asyncio.Handle):

    def __init__(self, callback, args, timer, loop):
        super().__init__(callback, args, loop)
        self._timer = timer

    def cancel(self):
        super().cancel()
        if self._timer and not self._timer.closed:
            # cancel() is not supposed to be thread safe, this is clearly
            # not either
            loop = self._timer.loop._rose_loop
            self._timer.close()
            loop._timers.remove(self._timer)
            del self._timer.handler
        self._timer = None


class EventLoop(base_events.BaseEventLoop):

    def __init__(self):
        super().__init__()
        self._loop = pyuv.Loop()
        self._loop._rose_loop = self
        self._default_executor = None
        self._last_exc = None
        self._running = False

        self._fd_map = {}
        self._signal_handlers = {}
        self._subprocesses = {}
        self._ready = collections.deque()
        self._timers = collections.deque()

        self._waker = pyuv.Async(self._loop, self._async_cb)
        self._ready_processor = pyuv.Idle(self._loop)

    def run_forever(self):
        if self._running:
            raise RuntimeError('Event loop is running.')
        self._running = True
        try:
            self._run(pyuv.UV_RUN_DEFAULT)
        finally:
            self._running = False

    def run_until_complete(self, future):
        _stop_callback = lambda x: self.stop()
        future = asyncio.async(future, loop=self)
        future.add_done_callback(_stop_callback)
        self.run_forever()
        future.remove_done_callback(_stop_callback)
        if not future.done():
            raise RuntimeError('Event loop stopped before Future completed.')
        return future.result()

    def stop(self):
        self._loop.stop()
        self._waker.send()

    def close(self):
        self._fd_map.clear()
        self._signal_handlers.clear()
        self._subprocesses.clear()
        self._ready.clear()
        self._timers.clear()

        handles = self._loop.handles
        for handle in handles:
            if not handle.closed:
                handle.close()

        # Run a loop iteration so that close callbacks are called and resources are freed
        self._loop.run(pyuv.UV_RUN_DEFAULT)

        del self._loop._rose_loop
        self._loop = None
        super().close()

    def is_running(self):
        return self._running

    # Methods scheduling callbacks. All these return Handles.

    def call_soon(self, callback, *args):
        handler = asyncio.Handle(callback, args, self)
        self._add_callback(handler)
        return handler

    def call_later(self, delay, callback, *args):
        if delay <= 0:
            return self.call_soon(callback, *args)
        timer = pyuv.Timer(self._loop)
        handler = TimerHandle(callback, args, timer, self)
        timer.handler = handler
        timer.start(self._timer_cb, delay, 0)
        self._timers.append(timer)
        return handler

    def call_at(self, when, callback, *args):
        return self.call_later(when - self.time(), callback, *args)

    def time(self):
        return time.monotonic()

    # Methods for interacting with threads.

    # run_in_executor - inherited from BaseEventLoop
    # set_default_executor - inherited from BaseEventLoop

    def call_soon_threadsafe(self, callback, *args):
        handler = asyncio.Handle(callback, args, self)
        # We don't use _add_callback here because starting the Idle handle
        # is not threadsafe. Instead, we queue the callback and in the Async
        # handle callback (which is run in the loop thread) we start the
        # Idle handle if needed
        self._ready.append(handler)
        self._waker.send()
        return handler

    # Network I/O methods returning Futures.

    # getaddrinfo - inherited from BaseEventLoop
    # getnameinfo - inherited from BaseEventLoop
    # create_connection - inherited from BaseEventLoop
    # create_datagram_endpoint - inherited from BaseEventLoop
    # connect_read_pipe - inherited from BaseEventLoop
    # connect_write_pipe - inherited from BaseEventLoop
    # start_serving - inherited from BaseEventLoop
    # start_serving_datagram - inherited from BaseEventLoop

    def _start_serving(self, protocol_factory, sock, sslcontext=None, server=None):
        # Needed by BaseEventLoop.start_serving
        self.add_reader(sock.fileno(), self._accept_connection, protocol_factory, sock, sslcontext, server)

    def _stop_serving(self, sock):
        self.remove_reader(sock.fileno())
        sock.close()

    def _accept_connection(self, protocol_factory, sock, sslcontext=None, server=None):
        try:
            conn, addr = sock.accept()
            conn.setblocking(False)
        except (BlockingIOError, InterruptedError):
            pass  # False alarm.
        except:
            # Bad error.  Stop serving.
            self.remove_reader(sock.fileno())
            sock.close()
            # There's nowhere to send the error, so just log it.
            # TODO: Someone will want an error handler for this.
            logger.exception('Accept failed')
        else:
            if sslcontext:
                self._make_ssl_transport(conn, protocol_factory(), sslcontext, None, server_side=True, extra={'peername': addr}, server=server)
            else:
                self._make_socket_transport(conn, protocol_factory(), extra={'peername': addr}, server=server)
        # It's now up to the protocol to handle the connection.

    def stop_serving(self, sock):
        self.remove_reader(sock.fileno())
        sock.close()

    # Ready-based callback registration methods.
    # The add_*() methods return None.
    # The remove_*() methods return True if something was removed,
    # False if there was nothing to delete.

    def add_reader(self, fd, callback, *args):
        handler = asyncio.Handle(callback, args, self)
        try:
            poll_h = self._fd_map[fd]
        except KeyError:
            poll_h = self._create_poll_handle(fd)
            self._fd_map[fd] = poll_h
        poll_h.pevents |= pyuv.UV_READABLE
        poll_h.read_handler = handler
        poll_h.start(poll_h.pevents, self._poll_cb)

    def remove_reader(self, fd):
        try:
            poll_h = self._fd_map[fd]
        except KeyError:
            return False
        else:
            handler = poll_h.read_handler
            poll_h.pevents &= ~pyuv.UV_READABLE
            poll_h.read_handler = None
            if poll_h.pevents == 0:
                del self._fd_map[fd]
                poll_h.close()
            else:
                poll_h.start(poll_h.pevents, self._poll_cb)
            if handler:
                handler.cancel()
                return True
            return False

    def add_writer(self, fd, callback, *args):
        handler = asyncio.Handle(callback, args, self)
        try:
            poll_h = self._fd_map[fd]
        except KeyError:
            poll_h = self._create_poll_handle(fd)
            self._fd_map[fd] = poll_h
        poll_h.pevents |= pyuv.UV_WRITABLE
        poll_h.write_handler = handler
        poll_h.start(poll_h.pevents, self._poll_cb)

    def remove_writer(self, fd):
        try:
            poll_h = self._fd_map[fd]
        except KeyError:
            return False
        else:
            handler = poll_h.write_handler
            poll_h.pevents &= ~pyuv.UV_WRITABLE
            poll_h.write_handler = None
            if poll_h.pevents == 0:
                del self._fd_map[fd]
                poll_h.close()
            else:
                poll_h.start(poll_h.pevents, self._poll_cb)
            if handler:
                handler.cancel()
                return True
            return False

    # Completion based I/O methods returning Futures.

    def sock_recv(self, sock, n):
        fut = asyncio.Future(loop=self)
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
        except (BlockingIOError, InterruptedError):
            self.add_reader(fd, self._sock_recv, fut, True, sock, n)
        except Exception as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(data)

    def sock_sendall(self, sock, data):
        fut = asyncio.Future(loop=self)
        if data:
            self._sock_sendall(fut, False, sock, data)
        else:
            fut.set_result(None)
        return fut

    def _sock_sendall(self, fut, registered, sock, data):
        fd = sock.fileno()
        if registered:
            self.remove_writer(fd)
        if fut.cancelled():
            return
        try:
            n = sock.send(data)
        except (BlockingIOError, InterruptedError):
            n = 0
        except Exception as exc:
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
        fut = asyncio.Future(loop=self)
        self._sock_connect(fut, False, sock, address)
        return fut

    def _sock_connect(self, fut, registered, sock, address):
        fd = sock.fileno()
        if registered:
            self.remove_writer(fd)
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
        except (BlockingIOError, InterruptedError):
            self.add_writer(fd, self._sock_connect, fut, True, sock, address)
        except Exception as exc:
            fut.set_exception(exc)

    def sock_accept(self, sock):
        fut = asyncio.Future(loop=self)
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
        except (BlockingIOError, InterruptedError):
            self.add_reader(fd, self._sock_accept, fut, True, sock)
        except Exception as exc:
            fut.set_exception(exc)

    # Signal handling.

    def add_signal_handler(self, sig, callback, *args):
        self._validate_signal(sig)
        signal_h = pyuv.Signal(self._loop)
        handler = asyncio.Handle(callback, args, self)
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

    def _make_socket_transport(self, sock, protocol, waiter=None, *, extra=None, server=None):
        return selector_events._SelectorSocketTransport(self, sock, protocol, waiter, extra, server)

    def _make_ssl_transport(self, rawsock, protocol, sslcontext, waiter, *, server_side=False, server_hostname=None, extra=None, server=None):
        return selector_events._SelectorSslTransport(self, rawsock, protocol, sslcontext, waiter, server_side, server_hostname, extra, server)

    def _make_datagram_transport(self, sock, protocol, address=None, extra=None):
        return selector_events._SelectorDatagramTransport(self, sock, protocol, address, extra)

    def _make_read_pipe_transport(self, pipe, protocol, waiter=None, extra=None):
        if sys.platform != 'win32':
            from asyncio import unix_events
            return unix_events._UnixReadPipeTransport(self, pipe, protocol, waiter, extra)
        raise NotImplementedError

    def _make_write_pipe_transport(self, pipe, protocol, waiter=None, extra=None):
        if sys.platform != 'win32':
            from asyncio import unix_events
            return unix_events._UnixWritePipeTransport(self, pipe, protocol, waiter, extra)
        # TODO
        raise NotImplementedError

    @asyncio.coroutine
    def _make_subprocess_transport(self, protocol, args, shell, stdin, stdout, stderr, bufsize, extra=None, **kwargs):
        if sys.platform == 'win32':
            # TODO
            raise NotImplementedError
        from asyncio import unix_events
        self._reg_sigchld()
        transp = unix_events._UnixSubprocessTransport(self, protocol, args, shell, stdin, stdout, stderr, bufsize, extra=None, **kwargs)
        self._subprocesses[transp.get_pid()] = transp
        yield from transp._post_init()
        return transp

    def _reg_sigchld(self):
        if signal.SIGCHLD not in self._signal_handlers:
            self.add_signal_handler(signal.SIGCHLD, self._sig_chld)

    def _sig_chld(self):
        try:
            while True:
                try:
                    pid, status = os.waitpid(0, os.WNOHANG)
                except ChildProcessError:
                    break
                if pid == 0:
                    continue
                elif os.WIFSIGNALED(status):
                    returncode = -os.WTERMSIG(status)
                elif os.WIFEXITED(status):
                    returncode = os.WEXITSTATUS(status)
                else:
                    # covered by
                    # SelectorEventLoopTests.test__sig_chld_unknown_status
                    # from tests/unix_events_test.py
                    # bug in coverage.py version 3.6 ???
                    continue  # pragma: no cover
                transp = self._subprocesses.get(pid)
                if transp is not None:
                    transp._process_exited(returncode)
        except Exception:
            logger.exception('Unknown exception in SIGCHLD handler')

    def _subprocess_closed(self, transport):
        pid = transport.get_pid()
        self._subprocesses.pop(pid, None)

    def _run(self, mode):
        r = self._loop.run(mode)
        if self._last_exc is not None:
            exc, self._last_exc = self._last_exc, None
            raise exc[1]
        return r

    def _add_callback(self, callback):
        self._ready.append(callback)
        if not self._ready_processor.active:
            self._ready_processor.start(self._process_ready)

    def _async_cb(self, async):
        if not self._ready_processor.active:
            self._ready_processor.start(self._process_ready)

    def _timer_cb(self, timer):
        assert not timer.handler._cancelled
        self._add_callback(timer.handler)
        del timer.handler
        self._timers.remove(timer)
        timer.close()

    def _signal_cb(self, signal_h, signum):
        if signal_h.handler._cancelled:
            self.remove_signal_handler(signum)
            return
        self._add_callback(signal_h.handler)

    def _poll_cb(self, poll_h, events, error):
        fd = poll_h.fileno()
        if error is not None:
            # An error happened, signal both readability and writability and
            # let the error propagate
            if poll_h.read_handler is not None:
                if poll_h.read_handler._cancelled:
                    self.remove_reader(fd)
                else:
                    self._add_callback(poll_h.read_handler)
            if poll_h.write_handler is not None:
                if poll_h.write_handler._cancelled:
                    self.remove_writer(fd)
                else:
                    self._add_callback(poll_h.write_handler)
            return

        old_events = poll_h.pevents
        modified = False

        if events & pyuv.UV_READABLE:
            if poll_h.read_handler is not None:
                if poll_h.read_handler._cancelled:
                    self.remove_reader(fd)
                    modified = True
                else:
                    self._add_callback(poll_h.read_handler)
            else:
                poll_h.pevents &= ~pyuv.UV_READABLE
        if events & pyuv.UV_WRITABLE:
            if poll_h.write_handler is not None:
                if poll_h.write_handler._cancelled:
                    self.remove_writer(fd)
                    modified = True
                else:
                    self._add_callback(poll_h.write_handler)
            else:
                poll_h.pevents &= ~pyuv.UV_WRITABLE

        if not modified and old_events != poll_h.pevents:
            # Rearm the handle
            poll_h.start(poll_h.pevents, self._poll_cb)

    def _process_ready(self, handle):
        # This is the only place where callbacks are actually *called*.
        # All other places just add them to ready.
        # Note: We run all currently scheduled callbacks, but not any
        # callbacks scheduled by callbacks run this time around --
        # they will be run the next time (after another I/O poll).
        # Use an idiom that is threadsafe without using locks.
        ntodo = len(self._ready)
        for i in range(ntodo):
            handler = self._ready.popleft()
            if not handler._cancelled:
                try:
                    handler._run()
                except BaseException:
                    self._last_exc = sys.exc_info()
                    self._loop.stop()
                    break
        handler = None  # break cycles when exception occurs
        if not self._ready:
            self._ready_processor.stop()

    def _create_poll_handle(self, fdobj):
        poll_h = pyuv.Poll(self._loop, self._fileobj_to_fd(fdobj))
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

