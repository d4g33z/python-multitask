import random
import sys
import Queue
import threading


class hackysacker(object):

    counter = 0
    done = threading.Event()

    def __init__(self, name, circle):
        self.name = name
        self.circle = circle
        circle.append(self)
        self.messageQueue = Queue.Queue()

    def incrementCounter(self):
        hackysacker.counter += 1
        if hackysacker.counter >= turns:
            while self.circle:
                self.circle.pop().messageQueue.put('exit')
            hackysacker.done.set()
            sys.exit()

    def messageLoop(self):
        while True:
            message = self.messageQueue.get()
            if message == 'exit':
                sys.exit()
            debugPrint("%s got hackeysack from %s" % (self.name, message.name))
            kickTo = self.circle[random.randint(0, len(self.circle) - 1)]
            while kickTo is self:
                kickTo = self.circle[random.randint(0, len(self.circle) - 1)]
            debugPrint("%s kicking hackeysack to %s" % (self.name, kickTo.name))
            self.incrementCounter()
            kickTo.messageQueue.put(self)


def debugPrint(x):
    if debug:
        print x


debug = 1
hackysackers = 5
turns = 1


def runit(hs=5, ts=5, dbg=1):
    global hackysackers, turns, debug
    hackysackers = hs
    turns = ts
    debug = dbg
    
    hackysacker.counter = 0
    circle = []

    one = hackysacker('1', circle)
    threading.Thread(target=one.messageLoop).start()

    for i in xrange(2, hackysackers + 1):
        threading.Thread(target=hackysacker(str(i),circle).messageLoop).start()

    one.messageQueue.put(one)

    hackysacker.done.wait()


if __name__ == "__main__":
    runit()
