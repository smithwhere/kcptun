#!/usr/bin/env python3
"""Verify bad CLI and JSON settings are rejected before serving connections."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--server', required=True, type=Path)
parser.add_argument('--client', required=True, type=Path)
args = parser.parse_args()


class InvalidConfigTests(unittest.TestCase):
    def assert_rejected(self, binary, settings, parameter):
        for source in ('cli', 'json'):
            with self.subTest(source=source), tempfile.TemporaryDirectory() as directory:
                # Loopback and ephemeral TCP ports avoid touching existing services.
                if binary == args.client:
                    config = {'localaddr': '127.0.0.1:0'}
                else:
                    config = {'listen': '127.0.0.1:29900'}
                config.update(settings)
                command = [str(binary.resolve())]
                if source == 'json':
                    path = Path(directory) / 'config.json'
                    path.write_text(json.dumps(config))
                    command += ['-c', str(path)]
                else:
                    for name, value in config.items():
                        flag = {'qpp': 'QPP', 'qpp-count': 'QPPCount'}.get(name, name)
                        command.append(f'--{flag}={str(value).lower()}')
                try:
                    result = subprocess.run(command, stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT, text=True,
                                            timeout=2)
                except subprocess.TimeoutExpired:
                    self.fail(f'invalid {parameter} was accepted; process stayed running')
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertNotIn('panic:', result.stdout, result.stdout)
                self.assertIn(parameter.lower(), result.stdout.lower(), result.stdout)
                self.assertNotIn('listening on:', result.stdout, result.stdout)

    def test_both_endpoints_reject_bad_fec(self):
        for binary in (args.client, args.server):
            for data, parity in ((-1, 3), (10, -1), (256, 1), (2147483648, 2147483648)):
                with self.subTest(binary=binary, data=data, parity=parity):
                    self.assert_rejected(binary, {'datashard': data, 'parityshard': parity}, 'FEC')

    def test_both_endpoints_reject_rate_limit_overflow(self):
        for binary in (args.client, args.server):
            for rate in (4294967296, 8589934592):
                with self.subTest(binary=binary, rate=rate):
                    self.assert_rejected(binary, {'ratelimit': rate}, 'ratelimit')

    def test_both_endpoints_reject_bad_mtu(self):
        for binary in (args.client, args.server):
            for mtu in (0, 40, 77, 1501):
                with self.subTest(binary=binary, mtu=mtu):
                    self.assert_rejected(binary, {'mtu': mtu}, 'MTU')

    def test_startup_errors_exit_nonzero(self):
        for binary in (args.client, args.server):
            for flag in ('--does-not-exist',):
                result = subprocess.run([str(binary.resolve()), flag], capture_output=True,
                                        text=True, timeout=2)
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        result = subprocess.run([str(args.server.resolve()), '--listen=invalid'],
                                capture_output=True, text=True, timeout=2)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_client_rejects_connection_count_outside_uint16_range(self):
        for value in (-1, 0, 65536, 65537, 131072):
            with self.subTest(conn=value):
                self.assert_rejected(args.client, {'conn': value}, 'conn')

    def test_both_endpoints_reject_qpp_count_outside_uint16_range(self):
        for binary in (args.client, args.server):
            for value in (0, 65536, 65537):
                with self.subTest(binary=binary, count=value):
                    self.assert_rejected(binary, {'qpp': True, 'qpp-count': value},
                                         'QPPCount')

    def test_both_endpoints_reject_nonpositive_keepalive(self):
        for binary in (args.client, args.server):
            for value in (-1, 0):
                with self.subTest(binary=binary, keepalive=value):
                    self.assert_rejected(binary, {'keepalive': value}, 'keep-alive')


if __name__ == '__main__':
    unittest.main(argv=[__file__], verbosity=2)
