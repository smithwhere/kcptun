#!/usr/bin/env python3
"""Real TCP -> KCP/UDP -> TCP test; supports Windows and Linux binaries."""
import argparse
import re
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path


def exchange(conn, data):
    conn.sendall(data)
    received = bytearray()
    while len(received) < len(data):
        chunk = conn.recv(len(data) - len(received))
        if not chunk:
            raise AssertionError('Existing connection was closed during rotation')
        received.extend(chunk)
    assert bytes(received) == data, 'Payload changed across tunnel'


def echo(conn):
    with conn:
        try:
            while chunk := conn.recv(65536):
                conn.sendall(chunk)
        except OSError:
            pass


def serve(listener):
    while True:
        try:
            conn, _ = listener.accept()
        except OSError:
            return
        threading.Thread(target=echo, args=(conn,), daemon=True).start()


def free_tcp_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def udp_pair():
    for port in range(36000, 50000, 2):
        with socket.socket(type=socket.SOCK_DGRAM) as a, socket.socket(type=socket.SOCK_DGRAM) as b:
            try:
                a.bind(('127.0.0.1', port))
                b.bind(('127.0.0.1', port + 1))
                return port
            except OSError:
                continue
    raise RuntimeError('No UDP port pair available')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', required=True)
    parser.add_argument('--server', required=True)
    args = parser.parse_args()
    processes = []
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    threading.Thread(target=serve, args=(listener,), daemon=True).start()
    local_port, remote_port = free_tcp_port(), udp_pair()
    with tempfile.TemporaryDirectory(prefix='kcp-rotation-') as tmp:
        logs = [Path(tmp) / 'server.log', Path(tmp) / 'client.log']
        handles = [p.open('wb') for p in logs]
        common = ['--key', 'rotation-integration-test', '--nocomp', '--mode', 'fast3']
        try:
            commands = [
                [args.server, '-l', f'127.0.0.1:{remote_port}-{remote_port+1}', '-t', f'127.0.0.1:{listener.getsockname()[1]}', '--closewait', '0', *common],
                [args.client, '-l', f'127.0.0.1:{local_port}', '-r', f'127.0.0.1:{remote_port}-{remote_port+1}', '--autoexpire', '1', '--scavengettl', '1', '--graceful', '--conn', '1', *common],
            ]
            for cmd, log in zip(commands, handles):
                processes.append(subprocess.Popen(cmd, stdout=log, stderr=log))
            deadline = time.monotonic() + 15
            while True:
                try:
                    long = socket.create_connection(('127.0.0.1', local_port), timeout=3)
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.1)
            with long:
                exchange(long, b'initial-long-lived-stream')
                initial = re.findall(r'on connection: (\S+) -> (\S+)', logs[1].read_text(errors='replace'))[-1]
                ports = [int(initial[1].rsplit(':', 1)[1])]
                # Exercise active and idle streams past multiple TTL/scavenger sweeps.
                for i in range(12):
                    time.sleep(1.1)
                    if i % 3 != 0:
                        exchange(long, bytes([i]) * 8192)
                    with socket.create_connection(('127.0.0.1', local_port), timeout=3) as short:
                        exchange(short, f'new-stream-{i}'.encode())
                    text = logs[1].read_text(errors='replace')
                    current = re.findall(r'on connection: (\S+) -> (\S+)', text)[-1]
                    port = int(current[1].rsplit(':', 1)[1])
                    assert port != ports[-1], 'New stream did not rotate to a different port'
                    ports.append(port)
                    assert f'retired session closed: {initial[0]}' not in text, 'Old active tunnel closed'
                exchange(long, b'original-socket-still-alive')
            deadline = time.monotonic() + 15
            while f'retired session closed: {initial[0]}' not in logs[1].read_text(errors='replace'):
                assert time.monotonic() < deadline, 'Drained old tunnel was not reclaimed'
                time.sleep(0.2)
            print(f'PASS: {len(ports)-1} port rotations; original TCP socket intact; old tunnel reclaimed after close')
        except Exception:
            for p in logs:
                print(p.name, p.read_text(errors='replace'))
            raise
        finally:
            for p in processes:
                p.terminate()
            for p in processes:
                p.wait(timeout=10)
            for h in handles:
                h.close()
            listener.close()


if __name__ == '__main__':
    main()


