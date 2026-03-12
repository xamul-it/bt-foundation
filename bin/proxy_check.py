#!/usr/bin/env python3
"""Verifica heartbeat del proxy ZMQ Alpaca su localhost:5555."""

import zmq

def check_proxy():
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect("tcp://localhost:5555")
    socket.send_multipart([b'', b'HEARTBEAT'])
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    socks = dict(poller.poll(2000))  # Timeout 2 secondi
    if socket in socks:
        reply_parts = socket.recv_multipart()
        if b'PONG' in reply_parts:
            print("Proxy Alpaca attivo")
            return True
        else:
            print("Risposta inaspettata dal proxy:", reply)
    else:
        print("Nessuna risposta dal proxy entro il timeout")
    print("Proxy Alpaca NON attivo")
    return False

if __name__ == "__main__":
    check_proxy()
