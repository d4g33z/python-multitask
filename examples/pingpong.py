import multitask


def pingponger(get_queue, put_queue, message):
    while True:
        print (yield get_queue.get())
        yield put_queue.put(message)


if __name__ == '__main__':
    ping_queue = multitask.Queue()
    pong_queue = multitask.Queue(['PING'])

    multitask.add(pingponger(ping_queue, pong_queue, 'PING'))
    multitask.add(pingponger(pong_queue, ping_queue, 'PONG'))
    multitask.run()
