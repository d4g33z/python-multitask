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



"""

Cooperative multitasking and asynchronous I/O using generators

multitask allows Python programs to use generators (a.k.a. coroutines)
to perform cooperative multitasking and asynchronous I/O.
Applications written using multitask consist of a set of cooperating
tasks that yield to a shared task manager whenever they perform a
(potentially) blocking operation, such as I/O on a socket or getting
data from a queue.  The task manager temporarily suspends the task
(allowing other tasks to run in the meantime) and then restarts it
when the blocking operation is complete.  Such an approach is suitable
for applications that would otherwise have to use select() and/or
multiple threads to achieve concurrency.

The functions and classes in the multitask module allow tasks to yield
for I/O operations on sockets and file descriptors, adding/removing
data to/from queues, or sleeping for a specified interval.  When
yielding, a task can also specify a timeout.  If the operation for
which the task yielded has not completed after the given number of
seconds, the task is restarted, and a Timeout exception is raised at
the point of yielding.

As a very simple example, here's how one could use multitask to allow
two unrelated tasks to run concurrently:

  >>> def printer(message):
  ...     for dummy in xrange(3):
  ...         print message
  ...         yield
  ... 
  >>> multitask.add(printer('hello'))
  >>> multitask.add(printer('goodbye'))
  >>> multitask.run()
  hello
  goodbye
  hello
  goodbye
  hello
  goodbye

For a more useful example, here's how one could implement a
multitasking server that can handle multiple concurrent client
connections:

  def listener(sock):
      while True:
          conn, address = (yield multitask.accept(sock))
          multitask.add(client_handler(conn))

  def client_handler(sock):
      while True:
          request = (yield multitask.recv(sock, 1024))
          if not request:
              break
          response = handle_request(request)
          yield multitask.send(sock, response)

  multitask.add(listener(sock))
  multitask.run()

Tasks can also yield other tasks, which allows for composition of
tasks and reuse of existing multitasking code.  A child task runs
until it either completes or raises an exception.  To return output to
its parent, a child task raises StopIteration, passing the output
value(s) to the StopIteration constructor.  An unhandled exception
raised within a child task is propagated to its parent.  For example:

  >>> def parent():
  ...     print (yield return_none())
  ...     print (yield return_one())
  ...     print (yield return_many())
  ...     try:
  ...         yield raise_exception()
  ...     except Exception, e:
  ...         print 'caught exception: %s' % e
  ... 
  >>> def return_none():
  ...     yield
  ...     # do nothing
  ...     # or return
  ...     # or raise StopIteration
  ...     # or raise StopIteration(None)
  ... 
  >>> def return_one():
  ...     yield
  ...     raise StopIteration(1)
  ... 
  >>> def return_many():
  ...     yield
  ...     raise StopIteration(2, 3)  # or raise StopIteration((2, 3))
  ... 
  >>> def raise_exception():
  ...     yield
  ...     raise RuntimeError('foo')
  ... 
  >>> multitask.add(parent())
  >>> multitask.run()
  None
  1
  (2, 3)
  caught exception: foo

"""


import collections
import errno
from functools import partial
import _heapq as heapq
import os
import select
import socket
import sys
import time
import types
import weakref


__author__ = 'Christopher Stawarz <cstawarz@gmail.com>'
__version__ = '0.3.0'



################################################################################
#
# Code-documenting decorators and utility functions
#
################################################################################



def task(func):
    """

    Do-nothing decorator used to indicate that the result of a
    function or method is a top-level task that should be added to the
    task manager

    """
    return func


def yieldable(func):
    """

    Do-nothing decorator used to indicate that the result of a
    function or method must be yielded to the task manager

    """
    return func


def maybe_yieldable(func):
    """

    Do-nothing decorator used to indicate that the result of a
    function or method may need to be yielded to the task manager.
    Use must_yield() to determine whether a specific result must be
    yielded.

    """
    return func


def must_yield(obj):
    """

    Return True if obj must be yielded to the task manager, False
    otherwise

    """
    return isinstance(obj, (types.GeneratorType, YieldCondition))



################################################################################
#
# Timeout exception type
#
################################################################################



class Timeout(Exception):
    'Raised in a yielding task when an operation times out'
    pass



################################################################################
#
# MetaYieldCondition metaclass
#
################################################################################



class MetaYieldCondition(type):

    'Metaclass for YieldCondition'

    __custom_wait_handlers = []

    def __init__(cls, cname, cbases, cdict):
        super(MetaYieldCondition, cls).__init__(cname, cbases, cdict)
        for meth in ('_has_waits', '_handle_waits', '_merge'):
            if meth not in cdict:
                break
        else:
            MetaYieldCondition.__custom_wait_handlers.append(cls)

    @staticmethod
    def _has_waits(tm):
        for cls in MetaYieldCondition.__custom_wait_handlers:
            if cls._has_waits(tm):
                return True
        return False

    @staticmethod
    def _handle_waits(tm, timeout=None):
        for cls in MetaYieldCondition.__custom_wait_handlers:
            if cls._has_waits(tm):
                cls._handle_waits(tm, tm._get_run_timeout(timeout))

    @staticmethod
    def _merge(tm1, tm2):
        for cls in MetaYieldCondition.__custom_wait_handlers:
            cls._merge(tm1, tm2)



################################################################################
#
# YieldCondition class
#
################################################################################



class YieldCondition(object):

    """

    Base class for objects that are yielded by a task to the task
    manager and specify the condition(s) under which the task should
    be restarted.

    """

    __metaclass__ = MetaYieldCondition

    def __init__(self, timeout=None):
        """

        If timeout is None, the task will be suspended indefinitely
        until the condition is met.  Otherwise, if the condition is
        not met within timeout seconds, a Timeout exception will be
        raised in the yielding task.

        """

        self.task = None
        if timeout is None:
            self.expiration = None
        else:
            self.expiration = time.time() + float(timeout)

    def _handle(self, tm):
        if self.expiration is not None:
            tm._add_timeout(self)

    def _handle_timeout(self, tm):
        tm._enqueue(self.task, exc_info=(Timeout,))

    def _reenqueue(self, tm, input=None, exc_info=()):
        tm._enqueue(self.task, input, exc_info)
        if self.expiration is not None:
            tm._remove_timeout(self)



################################################################################
#
# FDReady class and related functions and classes
#
################################################################################



def _is_file_descriptor(fd):
    return isinstance(fd, (int, long))


def _socket_error_from_errno(err):
    return socket.error(err, os.strerror(err))


if False and hasattr(select, 'poll'):

    #
    # poll-based selector
    #

    class _FDSelector(object):

        def __init__(self):
            self._poller = select.poll()
            self._waits = {}

        def __nonzero__(self):
            return bool(self._waits)

        def add(self, fd):
            eventmask = 0
            for add, flag in ((fd.read, select.POLLIN),
                              (fd.write, select.POLLOUT)):
                eventmask |= flag
            self._poller.register(fd, eventmask)
            self._waits[fd.fileno()] = fd

        def remove(self, fd):
            self._poller.unregister(fd)
            self._waits.pop(fd.fileno())

        def process(self, tm, timeout):
            try:
                ready = self._poller.poll(timeout)
            except (select.error, IOError, OSError), err:
                if err.args[0] != errno.EINTR:
                    raise
            else:
                for fd, event in ready:
                    fd = self._waits[fd]

                    if event & select.POLLNVAL:
                        err = errno.EINVAL
                    elif event & select.POLLHUP:
                        err = errno.ECONNRESET
                    else:
                        err = 0

                    if err == 0:
                        fd._reenqueue(tm)
                    else:
                        fd._reenqueue(tm,
                                      exc_info=(_socket_error_from_errno(err),))

        def merge(self, other):
            for fd in other._waits.itervalues():
                self.add(fd)

else:

    #
    # select-based selector
    #

    class _FDSelector(object):

        def __init__(self):
            self._read_waits = set()
            self._write_waits = set()
            self._exc_waits = set()

        def __nonzero__(self):
            return bool(self._read_waits or
                        self._write_waits or
                        self._exc_waits)

        def add(self, fd):
            for add, fdset in ((fd.read, self._read_waits),
                               (fd.write, self._write_waits),
                               (fd.exc, self._exc_waits)):
                if add:
                    fdset.add(fd)

        def remove(self, fd):
            for remove, fdset in ((fd.read, self._read_waits),
                                  (fd.write, self._write_waits),
                                  (fd.exc, self._exc_waits)):
                if remove:
                    fdset.remove(fd)

        def process(self, tm, timeout):
            # The error handling here is (mostly) borrowed from Twisted
            try:
                read_ready, write_ready, exc_ready = \
                    select.select(self._read_waits,
                                  self._write_waits,
                                  self._exc_waits,
                                  timeout)
            except (TypeError, ValueError):
                self._remove_bad_file_descriptors(tm)
            except (select.error, IOError, OSError), err:
                if err.args[0] == errno.EINTR:
                    pass
                elif ((err.args[0] == errno.EBADF) or
                      ((sys.platform == 'win32') and
                       (err.args[0] == errno.WSAENOTSOCK))):
                    self._remove_bad_file_descriptors(tm)
                else:
                    # Not an error we can handle, so die
                    raise  #pragma: no cover
            else:
                for fd in set(read_ready + write_ready + exc_ready):
                    fd._reenqueue(tm)

        def _remove_bad_file_descriptors(self, tm):
            for fd in (self._read_waits | self._write_waits | self._exc_waits):
                try:
                    while True:
                        try:
                            select.select([fd], [fd], [fd], 0.0)
                            break
                        except (select.error, IOError, OSError), err:
                            if err.args[0] != errno.EINTR:
                                raise
                except:
                    fd._reenqueue(tm, exc_info=sys.exc_info())

        def merge(self, other):
            self._read_waits.update(other._read_waits)
            self._write_waits.update(other._write_waits)
            self._exc_waits.update(other._exc_waits)


class _FDSelectorDict(weakref.WeakKeyDictionary):

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            value = _FDSelector()
            self[key] = value
        return value


class FDReady(YieldCondition):

    """

    A task that yields an instance of this class will be suspended
    until a specified file descriptor is ready for I/O.

    """

    # Keys are TaskManager instances
    _waits = _FDSelectorDict()

    def __init__(self, fd, read=False, write=False, exc=False, timeout=None):
        """

        Resume the yielding task when fd is ready for reading,
        writing, and/or "exceptional" condition handling.  fd can be
        any object accepted by select.select() (meaning an integer or
        an object with a fileno() method that returns an integer).
        Any exception raised by select() due to fd will be re-raised
        in the yielding task.

        If timeout is not None, a Timeout exception will be raised in
        the yielding task if fd is not ready after timeout seconds
        have elapsed.

        """

        super(FDReady, self).__init__(timeout)

        self.fd = (fd if _is_file_descriptor(fd) else fd.fileno())

        if not (read or write or exc):
            raise ValueError("'read', 'write', and 'exc' cannot all be false")
        self.read = read
        self.write = write
        self.exc = exc

    def fileno(self):
        'Return the file descriptor on which the yielding task is waiting'
        return self.fd

    def _handle(self, tm):
        self._waits[tm].add(self)
        super(FDReady, self)._handle(tm)

    def _handle_timeout(self, tm):
        self._waits[tm].remove(self)
        super(FDReady, self)._handle_timeout(tm)

    def _reenqueue(self, tm, input=None, exc_info=()):
        self._waits[tm].remove(self)
        super(FDReady, self)._reenqueue(tm, input, exc_info)

    @classmethod
    def _has_waits(cls, tm):
        return bool(cls._waits[tm])

    @classmethod
    def _handle_waits(cls, tm, timeout):
        if (timeout is None) or (timeout > 0.0):
            cls._waits[tm].process(tm, timeout)

    @classmethod
    def _merge(cls, tm1, tm2):
        cls._waits[tm1].merge(cls._waits[tm2])
        cls._waits[tm2] = cls._waits[tm1] 


@yieldable
def readable(fd, timeout=None):
    """

    A task that yields the result of this function will be resumed
    when fd is readable.  If timeout is not None, a Timeout exception
    will be raised in the yielding task if fd is not readable after
    timeout seconds have elapsed.  For example:

      try:
          yield readable(sock, timeout=5)
          data = sock.recv(1024)
      except Timeout:
          # No data after 5 seconds

    """

    return FDReady(fd, read=True, timeout=timeout)


@yieldable
def writable(fd, timeout=None):
    """

    A task that yields the result of this function will be resumed
    when fd is writable.  If timeout is not None, a Timeout exception
    will be raised in the yielding task if fd is not writable after
    timeout seconds have elapsed.  For example:

      try:
          yield writable(sock, timeout=5)
          nsent = sock.send(data)
      except Timeout:
          # Can't send after 5 seconds

    """

    return FDReady(fd, write=True, timeout=timeout)


@yieldable
def _fdaction(fd, func, args=(), kwargs={}, read=False, write=False, exc=False):
    timeout = kwargs.pop('timeout', None)

    yield FDReady(fd, read, write, exc, timeout)

    while True:
        try:
            raise StopIteration(func(*(args), **(kwargs)))
        except (socket.error, IOError, OSError), err:
            if err.args[0] != errno.EINTR:
                raise


@yieldable
def read(fd, *args, **kwargs):
    """

    A task that yields the result of this function will be resumed
    when fd is readable, and the value of the yield expression will be
    the result of reading from fd.  If a timeout keyword is given and
    is not None, a Timeout exception will be raised in the yielding
    task if fd is not readable after timeout seconds have elapsed.
    Other arguments will be passed to the read function (os.read() if
    fd is an integer, fd.read() otherwise).  For example:

      try:
          data = (yield read(fd, 1024, timeout=5))
      except Timeout:
          # No data after 5 seconds

    """

    func = (partial(os.read, fd) if _is_file_descriptor(fd) else fd.read)
    return _fdaction(fd, func, args, kwargs, read=True)


@yieldable
def write(fd, *args, **kwargs):
    """

    A task that yields the result of this function will be resumed
    when fd is writable, and the value of the yield expression will be
    the result of writing to fd.  If a timeout keyword is given and is
    not None, a Timeout exception will be raised in the yielding task
    if fd is not writable after timeout seconds have elapsed.  Other
    arguments will be passed to the write function (os.write() if fd
    is an integer, fd.write() otherwise).  For example:

      try:
          nbytes = (yield write(fd, data, timeout=5))
      except Timeout:
          # Can't write after 5 seconds

    """

    func = (partial(os.write, fd) if _is_file_descriptor(fd) else fd.write)
    return _fdaction(fd, func, args, kwargs, write=True)


@yieldable
def accept(sock, *args, **kwargs):
    """

    A task that yields the result of this function will be resumed
    when sock is readable, and the value of the yield expression will
    be the result of accepting a new connection on sock.  If a timeout
    keyword is given and is not None, a Timeout exception will be
    raised in the yielding task if sock is not readable after timeout
    seconds have elapsed.  Other arguments will be passed to
    sock.accept().  For example:

      try:
          conn, address = (yield accept(sock, timeout=5))
      except Timeout:
          # No connections after 5 seconds

    """

    return _fdaction(sock, sock.accept, args, kwargs, read=True)


@yieldable
def connect(sock, address, timeout=None):
    """

    A task that yields the result of this function will be resumed
    when sock is connected to the specified address.  If timeout is
    not None, a Timeout exception will be raised in the yielding task
    if sock is not connected after timeout seconds have elapsed.  For
    example:

      try:
          yield connect(sock, address, timeout=5)
      except Timeout:
          # Not connected after 5 seconds

    """

    #
    # References for non-blocking connect:
    #   http://www.scottklement.com/rpg/socktut/nonblocking.html
    #   http://www.developerweb.net/forum/showthread.php?p=13486
    #   http://msdn2.microsoft.com/en-us/library/ms737625.aspx
    #   http://itamarst.org/writings/win32sockets.html
    #

    sock_timeout = sock.gettimeout()
    if sock_timeout != 0.0:
        sock.setblocking(False)
    try:
        while True:
            try:
                sock.connect(address)
                return
            except socket.error, err:
                if ((err.args[0] == errno.EINPROGRESS) or
                    ((sys.platform == 'win32') and
                     (err.args[0] == errno.WSAEWOULDBLOCK))):
                    break
                elif err.args[0] != errno.EINTR:  #pragma: no cover
                    raise

        if sys.platform == 'win32':
            # If the connection fails, sock goes into the exc list.
            # You still need to use getsockopt() to get the error.
            yield FDReady(sock, write=True, exc=True,
                          timeout=timeout)  #pragma: no cover
        else:
            yield writable(sock, timeout=timeout)

        err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err != 0:
            raise _socket_error_from_errno(err)
    finally:
        if sock_timeout != 0.0:
            sock.settimeout(sock_timeout)


@yieldable
def recv(sock, *args, **kwargs):
    """

    A task that yields the result of this function will be resumed
    when sock is readable, and the value of the yield expression will
    be the result of receiving from sock.  If a timeout keyword is
    given and is not None, a Timeout exception will be raised in the
    yielding task if sock is not readable after timeout seconds have
    elapsed.  Other arguments will be passed to sock.recv().  For
    example:

      try:
          data = (yield recv(sock, 1024, timeout=5))
      except Timeout:
          # No data after 5 seconds

    """

    return _fdaction(sock, sock.recv, args, kwargs, read=True)


@yieldable
def recvfrom(sock, *args, **kwargs):
    """

    A task that yields the result of this function will be resumed
    when sock is readable, and the value of the yield expression will
    be the result of receiving from sock.  If a timeout keyword is
    given and is not None, a Timeout exception will be raised in the
    yielding task if sock is not readable after timeout seconds have
    elapsed.  Other arguments will be passed to sock.recvfrom().  For
    example:

      try:
          data, address = (yield recvfrom(sock, 1024, timeout=5))
      except Timeout:
          # No data after 5 seconds

    """

    return _fdaction(sock, sock.recvfrom, args, kwargs, read=True)


@yieldable
def send(sock, *args, **kwargs):
    """

    A task that yields the result of this function will be resumed
    when sock is writable, and the value of the yield expression will
    be the result of sending to sock.  If a timeout keyword is given
    and is not None, a Timeout exception will be raised in the
    yielding task if sock is not writable after timeout seconds have
    elapsed.  Other arguments will be passed to the sock.send().  For
    example:

      try:
          nsent = (yield send(sock, data, timeout=5))
      except Timeout:
          # Can't send after 5 seconds

    """

    return _fdaction(sock, sock.send, args, kwargs, write=True)


@yieldable
def sendto(sock, *args, **kwargs):
    """

    A task that yields the result of this function will be resumed
    when sock is writable, and the value of the yield expression will
    be the result of sending to sock.  If a timeout keyword is given
    and is not None, a Timeout exception will be raised in the
    yielding task if sock is not writable after timeout seconds have
    elapsed.  Other arguments will be passed to the sock.sendto().
    For example:

      try:
          nsent = (yield sendto(sock, data, address, timeout=5))
      except Timeout:
          # Can't send after 5 seconds

    """

    return _fdaction(sock, sock.sendto, args, kwargs, write=True)



################################################################################
#
# Queue and _QueueAction classes
#
################################################################################



class Queue(object):

    """

    A multi-producer, multi-consumer FIFO queue (similar to
    Queue.Queue) that can be used for exchanging data between tasks

    """

    def __init__(self, contents=(), maxsize=0):
        """

        Create a new Queue instance.  contents is a sequence (empty by
        default) containing the initial contents of the queue.  If
        maxsize is greater than 0, the queue will hold a maximum of
        maxsize items, and put() will block until space is available
        in the queue.

        """

        self.maxsize = int(maxsize)
        self._queue = collections.deque(contents)
        self._get_waits = collections.deque()
        self._put_waits = collections.deque()

    def __contains__(self, item):
        'Return True if item is in the queue, False otherwise'
        return (item in self._queue)

    def __len__(self):
        'Return the number of items in the queue'
        return len(self._queue)

    def _get(self):
        return self._queue.popleft()

    def _put(self, item):
        self._queue.append(item)

    def empty(self):
        'Return True if the queue is empty, False otherwise'
        return (len(self) == 0)

    def full(self):
        'Return True if the queue is full, False otherwise'
        return ((len(self) >= self.maxsize) if (self.maxsize > 0) else False)

    @yieldable
    def get(self, timeout=None):
        """

        A task that yields the result of this method will be resumed
        when an item is available in the queue, and the value of the
        yield expression will be the item.  If timeout is not None, a
        Timeout exception will be raised in the yielding task if an
        item is not available after timeout seconds have elapsed.  For
        example:

          try:
              item = (yield queue.get(timeout=5))
          except Timeout:
              # No item available after 5 seconds

        """

        return _QueueAction(self, timeout=timeout)

    @yieldable
    def put(self, item, timeout=None):
        """

        A task that yields the result of this method will be resumed
        when item has been added to the queue.  If timeout is not
        None, a Timeout exception will be raised in the yielding task
        if no space is available after timeout seconds have elapsed.
        For example:

          try:
              yield queue.put(item, timeout=5)
          except Timeout:
              # No space available after 5 seconds

        """

        return _QueueAction(self, item, timeout=timeout)


class _QueueAction(YieldCondition):

    NO_ITEM = object()

    def __init__(self, queue, item=NO_ITEM, timeout=None):
        super(_QueueAction, self).__init__(timeout)
        if not isinstance(queue, Queue):
            raise TypeError("'queue' must be a Queue instance")
        self.queue = queue
        self.item = item

    def _handle(self, tm):
        if self.item is self.NO_ITEM:
            # Action is a get
            if self.queue.empty():
                self.queue._get_waits.append(self)
                super(_QueueAction, self)._handle(tm)
            else:
                item = self.queue._get()
                self._reenqueue(tm, input=item)
                if self.queue._put_waits:
                    action = self.queue._put_waits.popleft()
                    self.queue._put(action.item)
                    action._reenqueue(tm)
        else:
            # Action is a put
            if self.queue.full():
                self.queue._put_waits.append(self)
                super(_QueueAction, self)._handle(tm)
            else:
                self.queue._put(self.item)
                self._reenqueue(tm)
                if self.queue._get_waits:
                    action = self.queue._get_waits.popleft()
                    item = self.queue._get()
                    action._reenqueue(tm, input=item)

    def _handle_timeout(self, tm):
        if self.item is self.NO_ITEM:
            self.queue._get_waits.remove(self)
        else:
            self.queue._put_waits.remove(self)
        super(_QueueAction, self)._handle_timeout(tm)



################################################################################
#
# Other yieldable functions
#
################################################################################



@yieldable
def acquire(lock,
            timeout = None,
            _min_sleep = 0.001,
            _max_sleep = 0.05,
            _multiplier = 2):

    """

    A task that yields the result of this function will be resumed
    either when lock has been acquired or, if timeout is not None,
    after timeout seconds have elapsed.  The value of the yield
    expression will be false if the timeout has expired, true
    otherwise.  For example:

      gotit = (yield acquire(lock, timeout=5))
      if gotit:
          # Acquired the lock
      else:
          # Can't acquire after 5 seconds

    """

    # Logic borrowed from threading._Condition.wait()

    if timeout is not None:
        endtime = time.time() + float(timeout)

    delay = _min_sleep / _multiplier

    while True:
        gotit = lock.acquire(False)
        if gotit:
            break
        if timeout is None:
            delay = min(delay * _multiplier, _max_sleep)
        else:
            remaining = endtime - time.time()
            if remaining <= 0.0:
                break
            delay = min(delay * _multiplier, remaining, _max_sleep)
        yield sleep(delay)

    raise StopIteration(gotit)


@yieldable
def sleep(seconds):
    """

    A task that yields the result of this function will be resumed
    after the specified number of seconds have elapsed.  For example:

      while too_early():
          yield sleep(5)  # Sleep for five seconds
      do_something()      # Done sleeping; get back to work

    """

    seconds = float(seconds)
    if seconds <= 0.0:
        raise ValueError("'seconds' must be greater than 0")
    try:
        yield YieldCondition(timeout=seconds)
    except Timeout:
        pass



################################################################################
#
# _ChildTask class
#
################################################################################



class _ChildTask(object):

    __slots__ = ('parent', 'send', 'throw')

    def __init__(self, parent, task):
        self.parent = parent
        self.send = task.send
        self.throw = task.throw



################################################################################
#
# TaskManager class
#
################################################################################



class TaskManager(object):

    """

    Engine for running a set of cooperatively-multitasking tasks
    within a single Python thread

    """

    def __init__(self):
        """

        Create a new TaskManager instance.  Generally, there will only
        be one of these per Python process.  If you want to run two
        existing instances simultaneously, merge them first, then run
        one or the other.

        """

        self._queue = collections.deque()
        self._timeouts = []

    def merge(self, other):
        """

        Merge this TaskManager with another.  After the merge, the two
        objects share the same (merged) internal data structures, so
        either can be used to manage the combined task set.

        """

        if not isinstance(other, TaskManager):
            raise TypeError("'other' must be a TaskManager instance")

        # Merge the data structures
        self._queue.extend(other._queue)
        self._timeouts.extend(other._timeouts)
        heapq.heapify(self._timeouts)

        # Make other reference the merged data structures.  This is
        # necessary because other's tasks may reference and use other
        # (e.g. to add a new task in response to an event).
        other._queue = self._queue
        other._timeouts = self._timeouts

        # Merge custom wait handlers
        MetaYieldCondition._merge(self, other)

    def add(self, task):
        'Add a new task (i.e. a generator instance) to the run queue'

        if not isinstance(task, types.GeneratorType):
            raise TypeError("'task' must be a generator")
        self._enqueue(task)

    def _enqueue(self, task, input=None, exc_info=()):
        self._queue.append((task, input, exc_info))

    def run(self):
        """

        Run until there are no tasks that are currently runnable,
        waiting to time out, or waiting for I/O.  Note that this
        method can block indefinitely (e.g. if there are only I/O
        waits and no timeouts).  If this is unacceptable, use
        run_next() instead.

        """

        while (self._queue or
               self._timeouts or
               MetaYieldCondition._has_waits(self)):

            while self._queue:
                self._run_next_task()

            MetaYieldCondition._handle_waits(self)

            if self._timeouts:
                self._handle_timeouts(self._get_run_timeout())

    def run_next(self, timeout=None):
        """

        Perform one iteration of the run cycle: run all currently
        runnable tasks, check whether any pending I/O operations can
        be performed, check whether any timeouts have expired, then
        run all newly runnable tasks.

        The timeout argument specifies the maximum time to wait for
        some task to become runnable.  If timeout is None and there
        are no currently runnable tasks, but there are tasks waiting
        to perform I/O or time out, then this method will block until
        at least one of the waiting tasks becomes runnable.  To
        prevent this method from blocking indefinitely, use timeout to
        specify the maximum number of seconds to wait.

        """

        while self._queue:
            self._run_next_task()

        MetaYieldCondition._handle_waits(self, timeout)

        if self._timeouts:
            self._handle_timeouts(self._get_run_timeout(timeout))

        while self._queue:
            self._run_next_task()

    def _get_run_timeout(self, timeout=None):
        if self._queue:
            # Don't block if there are tasks in the queue
            timeout = 0.0
        elif self._timeouts:
            # If there are timeouts, block only until the first expiration
            expiration_timeout = max(0.0, self._timeouts[0][0] - time.time())
            if (timeout is None) or (timeout > expiration_timeout):
                timeout = expiration_timeout
        return timeout

    def _add_timeout(self, item):
        heapq.heappush(self._timeouts, (item.expiration, item))

    def _remove_timeout(self, item):
        self._timeouts.remove((item.expiration, item))
        heapq.heapify(self._timeouts)

    def _handle_timeouts(self, timeout):
        if timeout > 0.0:
            time.sleep(timeout)

        current_time = time.time()

        while self._timeouts and (self._timeouts[0][0] <= current_time):
            item = heapq.heappop(self._timeouts)[1]
            item._handle_timeout(self)

    def _run_next_task(self):
        task, input, exc_info = self._queue.popleft()
        while True:
            try:
                if exc_info:
                    output = task.throw(*exc_info)
                else:
                    output = task.send(input)
            except StopIteration, e:
                if not isinstance(task, _ChildTask):
                    break
                else:
                    if not e.args:
                        output = None
                    elif len(e.args) == 1:
                        output = e.args[0]
                    else:
                        output = e.args
                    task, input, exc_info = task.parent, output, ()
            except:
                if isinstance(task, _ChildTask):
                    # Propagate exception to parent
                    task, input, exc_info = task.parent, None, sys.exc_info()
                else:
                    # No parent task, so just die
                    raise
            else:
                if isinstance(output, types.GeneratorType):
                    task, input, exc_info = _ChildTask(task, output), None, ()
                else:
                    if isinstance(output, YieldCondition):
                        output.task = task
                        output._handle(self)
                    else:
                        # Return any other output as input and send task to
                        # end of queue
                        self._enqueue(task, input=output)
                    break

    def has_runnable(self):
        """

        Return True if there are runnable tasks in the queue, False
        otherwise

        """
        return bool(self._queue)

    def has_timeouts(self):
        """

        Return True if there are tasks with pending timeouts, False
        otherwise

        """
        return bool(self._timeouts)

    def has_waits(self):
        """

        Return True if there are tasks waiting for I/O, False
        otherwise

        """
        return MetaYieldCondition._has_waits(self)



################################################################################
#
# Default TaskManager instance
#
################################################################################



_default_task_manager = None


def get_default_task_manager():
    'Return the default TaskManager instance'
    global _default_task_manager
    if _default_task_manager is None:
        _default_task_manager = TaskManager()
    return _default_task_manager


def add(task):
    'Add a task to the default TaskManager instance'
    get_default_task_manager().add(task)


def run():
    'Run the default TaskManager instance'
    get_default_task_manager().run()



################################################################################
#
# Doc testing
#
################################################################################



if __name__ == '__main__':  #pragma: no cover
    import doctest

    multitask = sys.modules['__main__']
    doctest.testmod()
