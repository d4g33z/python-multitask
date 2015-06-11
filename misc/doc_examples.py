import multitask


################################################################################


def printer(message):
    while True:
        print message
        yield

multitask.add(printer('hello'))
multitask.add(printer('goodbye'))
multitask.run()


################################################################################


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


################################################################################


def parent():
    print (yield return_none())
    print (yield return_one())
    print (yield return_many())
    try:
        yield raise_exception()
    except Exception, e:
        print 'caught exception: %s' % e

def return_none():
    yield
    # do nothing
    # or return
    # or raise StopIteration
    # or raise StopIteration(None)

def return_one():
    yield
    raise StopIteration(1)

def return_many():
    yield
    raise StopIteration(2, 3)  # or raise StopIteration((2, 3))

def raise_exception():
    yield
    raise RuntimeError('foo')

multitask.add(parent())
multitask.run()
