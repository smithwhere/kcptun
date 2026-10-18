#!/usr/bin/env python3
"""Exercise FIN forwarding through real localhost kcptun client/server binaries.

Run with --server and --client pointing to native (not cross-compiled) binaries.
No public listener, router, VPS, or third-party service is used.
"""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--server', required=True, type=Path)
parser.add_argument('--client', required=True, type=Path)
args = parser.parse_args()
PAYLOAD = bytes(range(256)) * 256
UPLOAD = b'complete-request-before-peer-FIN'


def free_port(kind):
    with socket.socket(socket.AF_INET, kind) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class CloseNotificationTests(unittest.TestCase):
    def round_trip(self, smuxver, closewait=None, qpp=False, after_fin=False, request_fin=False):
        processes = []
        peer = None
        results = {}
        errors = []
        with tempfile.TemporaryDirectory(prefix='kcptun-eof-') as directory:
            root = Path(directory)
            with socket.socket() as target:
                target.bind(('127.0.0.1', 0))
                target.listen(1)
                target.settimeout(10)

                def serve():
                    try:
                        conn, _ = target.accept()
                        with conn:
                            conn.settimeout(5)
                            data = bytearray()
                            while len(data) < len(UPLOAD):
                                chunk = conn.recv(len(UPLOAD) - len(data))
                                if not chunk:
                                    break
                                data.extend(chunk)
                            results['upload'] = bytes(data)
                            if request_fin and conn.recv(1) != b'':
                                raise AssertionError('request EOF was not forwarded')
                            conn.sendall(PAYLOAD)
                            results['fin_at'] = time.monotonic()
                            conn.shutdown(socket.SHUT_WR)
                            tail = bytearray()
                            while True:
                                chunk = conn.recv(4096)
                                if not chunk:
                                    break
                                tail.extend(chunk)
                            results['tail'] = bytes(tail)
                    except Exception as error:
                        errors.append(error)

                worker = threading.Thread(target=serve, daemon=True)
                worker.start()
                udp_port = free_port(socket.SOCK_DGRAM)
                local_port = free_port(socket.SOCK_STREAM)
                common = {'key': 'public-local-regression-test', 'crypt': 'none',
                          'nocomp': True, 'quiet': True, 'smuxver': smuxver, 'qpp': qpp}
                server = dict(common, listen=f'127.0.0.1:{udp_port}',
                              target=f'127.0.0.1:{target.getsockname()[1]}')
                if closewait is not None:
                    server['closewait'] = closewait
                client = dict(common, localaddr=f'127.0.0.1:{local_port}',
                              remoteaddr=f'127.0.0.1:{udp_port}')
                with (root / 'process.log').open('w+') as log:
                    try:
                        for role, binary, config in [('server', args.server, server),
                                                     ('client', args.client, client)]:
                            config_path = root / (role + '.json')
                            config_path.write_text(json.dumps(config))
                            processes.append(subprocess.Popen(
                                [str(binary.resolve()), '-c', str(config_path)],
                                stdin=subprocess.DEVNULL, stdout=log, stderr=log))
                        deadline = time.monotonic() + 5
                        while peer is None:
                            self.assertTrue(all(p.poll() is None for p in processes),
                                            'kcptun exited before accepting the test')
                            try:
                                peer = socket.create_connection(('127.0.0.1', local_port),
                                                                timeout=.2)
                            except OSError:
                                if time.monotonic() >= deadline:
                                    self.fail('kcptun client did not start listening')
                                time.sleep(.02)
                        peer.settimeout(3)
                        peer.sendall(UPLOAD)
                        if request_fin:
                            peer.shutdown(socket.SHUT_WR)
                        received = bytearray()
                        try:
                            while True:
                                chunk = peer.recv(16384)
                                if not chunk:
                                    break
                                received.extend(chunk)
                        except socket.timeout:
                            self.fail('EOF was not forwarded within 3 seconds after '
                                      'the target half-closed its connection')
                        elapsed = time.monotonic() - results['fin_at']
                        self.assertEqual(bytes(received), PAYLOAD,
                                         'forwarded data was truncated before EOF')
                        if closewait is None or closewait == 0:
                            self.assertLess(elapsed, 2,
                                            'default/zero closewait delayed peer EOF')
                        else:
                            self.assertGreaterEqual(elapsed, .85,
                                                    'explicit closewait was ignored')
                        if after_fin:
                            peer.sendall(PAYLOAD)
                        if not request_fin:
                            peer.shutdown(socket.SHUT_WR)
                        worker.join(3)
                        self.assertFalse(worker.is_alive(), 'reverse stream did not finish')
                        self.assertEqual(errors, [], 'target endpoint failed')
                        self.assertEqual(results.get('upload'), UPLOAD,
                                         'the upstream request was truncated')
                        if after_fin:
                            self.assertEqual(results.get('tail'), PAYLOAD, 'reverse upload after EOF was lost')
                        print(f' qpp={qpp} request_fin={request_fin} after_fin={after_fin} smux={smuxver} closewait={closewait} EOF={elapsed:.3f}s',
                              flush=True)
                    finally:
                        if peer is not None:
                            peer.close()
                        for process in reversed(processes):
                            if process.poll() is None:
                                process.terminate()
                                try:
                                    process.wait(timeout=2)
                                except subprocess.TimeoutExpired:
                                    process.kill()
                                    process.wait()
                        worker.join(1)
                        log.flush()

    def test_omitted_closewait_forwards_complete_response_without_delay(self):
        for smuxver in (1, 2):
            with self.subTest(smuxver=smuxver):
                self.round_trip(smuxver)

    def test_response_after_request_eof(self):
        for smuxver in (1, 2):
            for qpp in (False, True):
                with self.subTest(smuxver=smuxver, qpp=qpp):
                    self.round_trip(smuxver, closewait=0, qpp=qpp, request_fin=True)

    def test_reverse_upload_after_peer_fin(self):
        for smuxver in (1, 2):
            for qpp in (False, True):
                with self.subTest(smuxver=smuxver, qpp=qpp):
                    self.round_trip(smuxver, closewait=0, qpp=qpp, after_fin=True)

    def test_explicit_zero_forwards_complete_response_without_delay(self):
        self.round_trip(1, closewait=0)

    def test_explicit_positive_closewait_remains_supported(self):
        self.round_trip(1, closewait=1)


if __name__ == '__main__':
    unittest.main(argv=[__file__], verbosity=2)
