import random
import sys
from multitask import TaskManager, Queue


class hackysacker(object):

    counter = 0

    def __init__(self, name, circle):
        self.name = name
        self.circle = circle
        circle.append(self)
        self.queue = Queue()

    def incrementCounter(self):
        hackysacker.counter += 1
        if hackysacker.counter >= turns:
            while self.circle:
                yield self.circle.pop().queue.put('exit')

    def messageLoop(self):
        while True:
            message = (yield self.queue.get())
            if message == 'exit':
                return
            debugPrint("%s got hackeysack from %s" % (self.name, message.name))
            kickTo = self.circle[random.randint(0, len(self.circle) - 1)]
            while kickTo is self:
                kickTo = self.circle[random.randint(0, len(self.circle) - 1)]
            debugPrint("%s kicking hackeysack to %s" % (self.name, kickTo.name))
            for message in self.incrementCounter():
                yield message
            yield kickTo.queue.put(self)


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

    tm = TaskManager()

    one = hackysacker('1', circle)
    tm.add(one.messageLoop())

    for i in xrange(2, hackysackers + 1):
        tm.add(hackysacker(str(i), circle).messageLoop())

    one.queue = Queue([one])

    tm.run()
    

if __name__ == "__main__":
    runit()
