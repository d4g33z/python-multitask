from __future__ import with_statement
from contextlib import closing
import socket
import sys

import multitask


def echo_client(hostname, port):
    err = 'getaddrinfo() returned no results'
    for addrinfo in socket.getaddrinfo(hostname,
                                       port,
                                       socket.AF_UNSPEC,
                                       socket.SOCK_STREAM,
                                       0,
                                       getattr(socket, 'AI_ADDRCONFIG', 0)):
        family, socktype, proto, canonname, sockaddr = addrinfo
        sock = socket.socket(family, socktype, proto)
        try:
            yield multitask.connect(sock, sockaddr)
            break
        except socket.error, err:
            sock.close()
    else:
        raise socket.error(err)

    with closing(sock):
        while True:
            if sys.platform == 'win32':
                input = sys.stdin.readline()
            else:
                input = (yield multitask.read(sys.stdin.fileno(), 1024))

            yield multitask.send(sock, input)
            output = (yield multitask.recv(sock, 1024))

            if sys.platform == 'win32':
                sys.stdout.write(output)
            else:
                yield multitask.write(sys.stdout.fileno(), output)


if __name__ == '__main__':
    hostname = None
    port = 1111

    if len(sys.argv) > 1:
        hostname = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])

    multitask.add(echo_client(hostname, port))
    try:
        multitask.run()
    except KeyboardInterrupt:
        pass
