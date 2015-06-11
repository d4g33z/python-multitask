################################################################################
#
# Copyright (c) 2007-2008 Christopher J. Stawarz
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
################################################################################



import gc
import select
import signal
import sys
try:
    import threading
except ImportError:  #pragma: no cover
    threading = None
import time
import unittest

import multitask
from multitask import *


class SetupMixin(object):

    def setUp(self):
        self.gc_enabled = gc.isenabled()
        if self.gc_enabled:
            gc.disable()
        self.tm = TaskManager()
        self.result = []

    def tearDown(self):
        del self.tm, self.result
        if self.gc_enabled:
            gc.enable()

    def add(self, func):
        self.tm.add(func())

    def add_run(self, func):
        self.add(func)
        self.tm.run()

    def assertResult(self, expected):
        self.assertEqual(self.result, expected)


class TestBasic(SetupMixin, unittest.TestCase):

    def test_simple_yield(self):
        num = 3

        def actor(x):
            for dummy in xrange(num):
                self.result.append(x)
                yield

        for i in xrange(1, num+1):
            self.tm.add(actor(i))
        self.tm.run()

        self.assertResult(range(1, num+1) * num)

    def test_sleep(self):
        @self.add
        def badsleeper():
            for t in (-1.0, 0.0):
                try:
                    yield sleep(t)
                except ValueError:
                    self.result.append(t)

        @self.add
        def sleeper():
            yield sleep(0.1)
            self.result.append('sleeper')

        @self.add_run
        def nonsleeper():
            yield
            self.result.append('nonsleeper')

        self.assertResult([-1.0, 0.0, 'nonsleeper', 'sleeper'])

    def test_child_task(self):
        def child(exit_exc=None, raise_exc=False):
            yield
            if raise_exc:
                raise RuntimeError('foo')
            raise exit_exc

        @self.add_run
        def parent():
            for exit_exc in (StopIteration(),
                             StopIteration(1.0),
                             StopIteration(1, 2, 3)):
                self.result.append((yield child(exit_exc)))
            try:
                yield child(raise_exc=True)
            except RuntimeError, e:
                self.result.append(e.args[0])

        self.assertResult([None, 1.0, (1, 2, 3), 'foo'])

    def test_bad_task(self):
        self.assertRaises(TypeError, self.tm.add, 3)

    def test_cleanup(self):
        tm = TaskManager()

        def f():
            yield readable(sys.stdin.fileno())

        tm.add(f())
        tm.run_next(0.0)

        self.assert_(tm.has_waits())
        del tm
        gc.collect()
        self.failIf(FDReady._waits)

    def test_bad_merge(self):
        self.assertRaises(TypeError, self.tm.merge, 3)

    def test_merge(self):
        tm1 = TaskManager()

        def task1():
            yield
            self.result.append('task1')

        def timeout1():
            yield sleep(0.2)
            self.result.append('timeout1')

        def badfd1():
            try:
                yield readable(sys.stdin.fileno(), timeout=0.01)
            except Timeout:
                self.result.append('badfd1')

        tm1.add(timeout1())
        tm1.add(badfd1())
        tm1.run_next(0.0)
        tm1.add(task1())

        tm2 = TaskManager()

        def task2():
            yield
            self.result.append('task2')

        def timeout2():
            yield sleep(0.1)
            self.result.append('timeout2')

        def badfd2():
            try:
                yield readable(sys.stdin.fileno(), timeout=0.02)
            except Timeout:
                self.result.append('badfd2')

        tm2.add(timeout2())
        tm1.add(badfd2())
        tm2.run_next(0.0)
        tm2.add(task2())

        self.failIf(self.result)

        tm1.merge(tm2)
        tm2.run()

        self.assertResult(['task1', 'task2', 'badfd1', 'badfd2', 'timeout2',
                           'timeout1'])

    def test_unhandled_exception(self):
        @self.add
        def t():
            yield
            raise RuntimeError('badness')

        self.assertRaises(RuntimeError, self.tm.run)


class SocketSetupMixin(SetupMixin):

    def setUp(self):
        super(SocketSetupMixin, self).setUp()
        self.s1, self.s2 = self._socketpair()
        self.addr1 = self.s1.getsockname()
        self.addr2 = self.s2.getsockname()

    def tearDown(self):
        self.s1.close()
        self.s2.close()
        del self.s1, self.s2, self.addr1, self.addr2
        super(SocketSetupMixin, self).tearDown()

    def _socketpair(self):
        listen_sock = socket.socket()
        listen_sock.bind(('localhost', 0))
        listen_sock.listen(1)

        socks = []

        def server():
            sock, addr = (yield accept(listen_sock))
            socks.append(sock)
            listen_sock.close()

        def client():
            sock = socket.socket()
            yield connect(sock, listen_sock.getsockname())
            socks.append(sock)

        tm = TaskManager()
        tm.add(server())
        tm.add(client())
        tm.run()
        del tm

        return socks


class TestFDReady(SocketSetupMixin, unittest.TestCase):

    def test_all_false(self):
        self.assertRaises(ValueError, FDReady, 0)

    def test_bad_file_descriptor(self):
        def t(fd, exc, timeout):
            try:
                yield FDReady(fd, True, True, True, timeout=timeout)
            except exc:
                self.result.append(fd)

        class C(object):
            def fileno(self):
                return -1.0
        c = C()

        for fd, exc, timeout in ((12, select.error, None),
                                 (sys.maxint, (ValueError, select.error), 0.1),
                                 (self.s1, ValueError, 0.1),
                                 (c, TypeError, None)):
            self.tm.add(t(fd, exc, timeout))

        self.failIf(self.tm.has_waits())
        self.tm.run_next()
        self.assert_(self.tm.has_waits())
        self.tm.run()
        self.failIf(self.tm.has_waits())

        self.result.sort()
        self.assertResult([12, sys.maxint, c])

    def test_bad_file_descriptor_2(self):
        # This is necessary because, for some reason,
        # test_bad_file_descriptor doesn't trigger select.error with
        # an errno of EBADF

        @self.add_run
        def badfd1():
            try:
                yield readable(13)
            except select.error:
                self.result.append('badfd1')

        self.assertResult(['badfd1'])

    def test_timeout(self):
        def t(func, sock):
            try:
                yield func(sock, timeout=0.1)
            except Timeout:
                self.result.append((sock, 'timeout'))
            else:
                self.result.append((sock, 'notimeout'))

        for sock, func in ((self.s1, readable), (self.s2, writable)):
            self.tm.add(t(func, sock))

        self.assert_(self.tm.has_runnable())
        self.failIf(self.tm.has_timeouts())
        self.failIf(self.tm.has_waits())

        self.tm.run_next()
        self.failIf(self.tm.has_runnable())
        self.assert_(self.tm.has_timeouts())
        self.assert_(self.tm.has_waits())

        self.tm.run()
        self.failIf(self.tm.has_runnable())
        self.failIf(self.tm.has_timeouts())
        self.failIf(self.tm.has_waits())

        self.assertResult([(self.s2, 'notimeout'), (self.s1, 'timeout')])

    def test_readable_writable(self):
        @self.add
        def reader():
            yield readable(self.s1)
            data = self.s1.recv(16)
            self.result.append(('reader', data))

        @self.add_run
        def writer():
            yield writable(self.s2)
            nbytes = self.s2.send('four')
            self.result.append(('writer', nbytes))

        self.assertResult([('writer', 4), ('reader', 'four')])

    if hasattr(signal, 'alarm'):
        def test_eintr(self):
            signal.signal(signal.SIGALRM, (lambda x,y:  None))
            signal.alarm(1)

            @self.add_run
            def t():
                try:
                    yield readable(self.s1, timeout=2)
                except Timeout:
                    self.result.append(None)

            self.assertResult([None])


class TestFDAction(SocketSetupMixin, unittest.TestCase):

    def test_processing_error(self):
        class C(object):
            def __init__(self, fd):
                self.fd = fd
            def fileno(self):
                return self.fd
            def write(self):
                raise IOError('foo')

        @self.add_run
        def t():
            try:
                yield write(C(self.s1.fileno()))
            except IOError, e:
                self.result.append(e.args[0])

        self.assertResult(['foo'])

    if sys.platform != 'win32':
        def test_read_write(self):
            @self.add
            def reader():
                data = (yield read(self.s1.fileno(), 16))
                self.result.append((self.s1, data))

            @self.add_run
            def writer():
                nbytes = (yield write(self.s2.fileno(), 'four'))
                self.result.append((self.s2, nbytes))

            self.assertResult([(self.s2, 4), (self.s1, 'four')])

    def test_accept_connect(self):
        s1 = socket.socket()
        s2 = socket.socket()
        s3 = socket.socket()
        try:
            @self.add
            def server():
                s1.bind(('localhost', 0))
                s1.listen(1)
                conn, address = (yield accept(s1))
                conn.close()
                s1.close()

            def other(msg):
                self.result.append(msg)
                yield

            @self.add
            def client():
                self.tm.add(other('other1'))
                yield connect(s2, s1.getsockname())
                self.result.append(s2)

            @self.add_run
            def denied_client():
                yield
                self.tm.add(other('other2'))
                try:
                    yield connect(s3, s1.getsockname())
                except socket.error:
                    self.result.append(s3)

            self.assertResult(['other1', 'other2', s2, s3])
        finally:
            s1.close()
            s2.close()
            s3.close()

    def test_connect_noblock(self):
        def other():
            self.result.append('other')
            yield

        @self.add_run
        def connector():
            self.tm.add(other())
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                yield connect(s, ('127.0.0.1', 1234))
                self.result.append('connector')
            finally:
                s.close()

        self.assertResult(['connector', 'other'])

    def test_send_recv(self):
        @self.add
        def receiver():
            data = (yield recv(self.s1, 16))
            self.result.append((self.s1, data))

        @self.add_run
        def sender():
            nbytes = (yield send(self.s2, 'four'))
            self.result.append((self.s2, nbytes))

        self.assertResult([(self.s2, 4), (self.s1, 'four')])

    def test_sendto_recvfrom(self):
        s1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s1.bind(('localhost', 0))
            s2.bind(('localhost', 0))

            @self.add
            def receiver():
                data, addr = (yield recvfrom(s1, 16))
                self.result.append((s1, data, addr))

            @self.add_run
            def sender():
                nbytes = (yield sendto(s2, 'four', s1.getsockname()))
                self.result.append((s2, nbytes))

            self.assertResult([(s2, 4), (s1, 'four', s2.getsockname())])
        finally:
            s1.close()
            s2.close()


class TestQueue(SetupMixin, unittest.TestCase):

    def test_basic(self):
        q = Queue()
        self.assertEqual(len(q), 0)
        self.assert_(q.empty())

        self.assertEqual(len(Queue(xrange(10))), 10)
        self.assert_(Queue([None], maxsize=1).full())

    def test_contains(self):
        q = Queue()
        self.failIf(1 in q)
        q = Queue([1])
        self.assert_(1 in q)

    def test_get_wait(self):
        queue = Queue()
        msg = 7

        @self.add
        def getter():
            self.result.append((yield queue.get(timeout=1.0)))

        @self.add_run
        def putter():
            yield queue.put(msg)

        self.assertResult([msg])

    def test_put_wait(self):
        queue = Queue([None], maxsize=1)

        @self.add
        def putter():
            yield queue.put(None, timeout=1.0)
            self.result.append('putter')

        @self.add_run
        def getter():
            yield queue.get()
            self.result.append('getter')

        self.assertResult(['getter', 'putter'])

    def test_timeout(self):
        @self.add
        def get_timeout():
            try:
                yield Queue().get(timeout=0.1)
            except Timeout:
                self.result.append('get')

        @self.add_run
        def put_timeout():
            try:
                yield Queue([None], maxsize=1).put(None, timeout=0)
            except Timeout:
                self.result.append('put')

        self.assertResult(['put', 'get'])

    def test_broken_queue_action(self):
        self.assertRaises(TypeError, multitask._QueueAction, 3)

    def test_run_order(self):
        queue = Queue([1, 2, 3], maxsize=3)

        @self.add
        def other():
            yield
            self.result.append('other')

        @self.add
        def getter():
            for dummy in xrange(3):
                self.result.append((yield queue.get()))

        @self.add_run
        def putter():
            for i in xrange(4, 7):
                yield queue.put(None)
                self.result.append(i)

        #self.assertResult([1, 2, 3, 4, 5, 6, 'other'])
        self.assertResult(['other', 1, 4, 2, 5, 3, 6])


if threading:
    class TestThreading(SetupMixin, unittest.TestCase):

        def test_acquire(self):
            lock = threading.Lock()

            @self.add
            def need_lock():
                gotit = (yield acquire(lock))
                if gotit:
                    self.result.append('need_lock')
                lock.release()

            @self.add
            def no_lock():
                yield sleep(0.1)
                self.result.append('no_lock')

            def target():
                lock.acquire()
                time.sleep(0.5)
                lock.release()

            t = threading.Thread(target=target)
            t.start()
            time.sleep(0.1)  # Let the thread acquire the lock
            self.tm.run()

            self.assertResult(['no_lock', 'need_lock'])

        def test_timeout(self):
            lock = threading.Lock()
            lock.acquire()

            @self.add_run
            def timeout():
                self.result.append((yield acquire(lock, timeout=0.1)))

            self.assertResult([False])


class TestMisc(unittest.TestCase):

    def test_decorators(self):
        def f():  pass
        for deco in (task, yieldable, maybe_yieldable):
            self.assert_(deco(f) is f)

    def test_must_yield(self):
        def f():  yield 1
        self.assert_(must_yield(f()))
        self.assert_(must_yield(YieldCondition()))
        self.failIf(must_yield(3))

    def test_default_task_manager(self):
        tm = get_default_task_manager()
        self.assert_(isinstance(tm, TaskManager))
        self.assert_(tm is get_default_task_manager())

        result = []

        def f():
            yield
            result.append(1)

        add(f())
        run()

        self.assertEqual(result, [1])


if __name__ == '__main__':
    unittest.main()
