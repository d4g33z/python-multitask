from __future__ import with_statement
from contextlib import closing
import errno
import socket

import multitask


GETADDRINFO_FLAGS = socket.AI_PASSIVE | getattr(socket, 'AI_ADDRCONFIG', 0)


def echo_server(hostname, port):
    for addrinfo in socket.getaddrinfo(hostname,
                                       port,
                                       socket.AF_UNSPEC,
                                       socket.SOCK_STREAM,
                                       0,
                                       GETADDRINFO_FLAGS):
        family, socktype, proto, canonname, sockaddr = addrinfo
        sock = socket.socket(family, socktype, proto)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(sockaddr)
            sock.listen(socket.SOMAXCONN)
            multitask.add(accept_connections(sock))
        except socket.error, err:
            sock.close()
            if err[0] != errno.EADDRINUSE:
                raise
        except:
            sock.close()
            raise


def accept_connections(sock):
    with closing(sock):
        while True:
            conn = (yield multitask.accept(sock))
            multitask.add(handle_connection(*conn))


def handle_connection(sock, address):
    with closing(sock):
        while True:
            data = (yield multitask.recv(sock, 1024))
            if not data:
                break
            yield multitask.send(sock, data)


if __name__ == '__main__':
    import sys

    hostname = None
    port = 1111

    if len(sys.argv) > 1:
        hostname = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])

    echo_server(hostname, port)
    try:
        multitask.run()
    except KeyboardInterrupt:
        pass
