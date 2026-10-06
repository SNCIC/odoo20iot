#!/usr/bin/env python3
import selectors
import socket
import sys
import threading


LISTENS = (("192.168.127.163", 18094), ("100.64.0.3", 18094))
TARGET = ("127.0.0.1", 18094)


def relay(client, upstream):
    sockets = ((client, upstream), (upstream, client))
    try:
        def forward(source, destination):
            try:
                while True:
                    data = source.recv(65536)
                    if not data:
                        break
                    destination.sendall(data)
            except (ConnectionError, OSError):
                pass
            finally:
                try:
                    destination.shutdown(socket.SHUT_WR)
                except OSError:
                    pass

        threads = [threading.Thread(target=forward, args=pair, daemon=True) for pair in sockets]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    except (ConnectionError, OSError):
        return
    finally:
        client.close()
        upstream.close()


def main():
    listeners = []
    for address in LISTENS:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(address)
        listener.listen(64)
        listeners.append(listener)
        print(f"listening on {address[0]}:{address[1]} -> {TARGET[0]}:{TARGET[1]}", flush=True)
    selector = selectors.DefaultSelector()
    for listener in listeners:
        selector.register(listener, selectors.EVENT_READ)
    while True:
        events = selector.select()
        for key, _ in events:
            client, address = key.fileobj.accept()
            client.settimeout(10)
            try:
                upstream = socket.create_connection(TARGET, timeout=5)
            except OSError as error:
                print(f"upstream failed for {address}: {error}", file=sys.stderr, flush=True)
                client.close()
                continue
            client.settimeout(None)
            threading.Thread(target=relay, args=(client, upstream), daemon=True).start()


if __name__ == "__main__":
    main()
