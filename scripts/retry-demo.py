#!/usr/bin/env python3
"""Run real Fluent Bit against a local, deliberately incomplete AWS API stub.

No real credentials or AWS calls. Accept stream A, then reject stream C creation
once. Record whether retry resends accepted records. This isolates a mechanism;
it does not measure AWS quotas or explain every duplicate in the cloud run.
"""
from collections import Counter
import argparse
import datetime as dt
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--no-throttle', action='store_true', help='Control: accept every request')
    args = parser.parse_args()
    out = ROOT / 'results' / ('retry-demo-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    out.mkdir(parents=True)
    trace, accepted, streams = [], [], set()
    rejected = False

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_POST(self):
            nonlocal rejected
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            operation = self.headers.get('X-Amz-Target', '').split('.')[-1]
            status, body = 200, {}
            if operation == 'CreateLogStream':
                stream = request['logStreamName']
                if stream == 'C' and not rejected and not args.no_throttle:
                    rejected = True
                    status, body = 400, {'__type': 'ThrottlingException', 'message': 'Injected one-time stream creation throttle'}
                elif stream in streams:
                    status, body = 400, {'__type': 'ResourceAlreadyExistsException', 'message': 'Already exists'}
                else:
                    streams.add(stream)
            elif operation == 'PutLogEvents':
                accepted.extend(json.loads(e['message'])['record_id'] for e in request['logEvents'])
                body = {'nextSequenceToken': 'unused'}
            else:
                status, body = 400, {'__type': 'UnsupportedOperation', 'message': operation}
            row = {'time': time.time(), 'operation': operation, 'request': request, 'status': status, 'response': body}
            trace.append(row)
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/x-amz-json-1.1')
            self.send_header('x-amzn-RequestId', f'local-request-{len(trace)}')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args):
            pass

    # The pinned output binary initializes a TLS context even with tls Off.
    # Use a private local CA certificate, trusted only by this test container.
    temporary = tempfile.TemporaryDirectory(prefix='cwlogs-retry-')
    cert, key = Path(temporary.name) / 'cert.pem', Path(temporary.name) / 'key.pem'
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                    '-keyout', str(key), '-out', str(cert), '-days', '1',
                    '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1'],
                   check=True, capture_output=True)
    (out / 'local-ca.pem').write_bytes(cert.read_bytes())
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert, key)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        input_port = sock.getsockname()[1]
    config = f'''[SERVICE]
    Flush 1
    Log_Level info
    scheduler.base 1
    scheduler.cap 2
[INPUT]
    Name http
    Listen 127.0.0.1
    Port {input_port}
[OUTPUT]
    Name cloudwatch_logs
    Match *
    region us-west-2
    log_group_name local-only
    log_stream_name fallback
    log_stream_template $stream
    endpoint 127.0.0.1
    port {server.server_port}
    tls Off
    tls.ca_file /config/local-ca.pem
    auto_create_group false
    auto_retry_requests false
    workers 1
    Retry_Limit False
'''
    (out / 'fluent-bit.conf').write_text(config)
    name = 'cwlogs-retry-' + str(int(time.time()))
    logfile = (out / 'fluent.log').open('w')
    process = None
    try:
        process = subprocess.Popen(['docker', 'run', '--rm', '--name', name, '--network', 'host',
                                    '-e', 'AWS_ACCESS_KEY_ID=local-test-key', '-e', 'AWS_SECRET_ACCESS_KEY=local-test-secret',
                                    '-e', 'AWS_EC2_METADATA_DISABLED=true',
                                    '-v', f'{out}:/config:ro', 'cr.fluentbit.io/fluent/fluent-bit:5.1.2',
                                    '-c', '/config/fluent-bit.conf'], stdout=logfile, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 20
        while True:
            if process.poll() is not None:
                raise RuntimeError('Fluent Bit exited: inspect fluent.log')
            try:
                with socket.create_connection(('127.0.0.1', input_port), timeout=1):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError('Fluent Bit HTTP input did not start')
                time.sleep(.2)
        records = [{'record_id': f'record-{s}', 'stream': s} for s in ['A', 'B', 'C']]
        request = urllib.request.Request(f'http://127.0.0.1:{input_port}/demo', data=json.dumps(records).encode(),
                                         headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status in (200, 201)
        deadline = time.monotonic() + 20
        while set(accepted) != {'record-A', 'record-B', 'record-C'} and time.monotonic() < deadline:
            time.sleep(.2)
        counts = dict(Counter(accepted))
        throttle_index = next((i for i, r in enumerate(trace) if r['status'] == 400 and r['response'].get('__type') == 'ThrottlingException'), None)
        before = [] if throttle_index is None else [r for r in trace[:throttle_index] if r['operation'] == 'PutLogEvents']
        passed = (counts == {'record-A': 1, 'record-B': 1, 'record-C': 1} if args.no_throttle else
                  counts == {'record-A': 2, 'record-B': 2, 'record-C': 1} and len(before) == 1 and rejected)
        result = {'control_without_throttle': args.no_throttle,
                  'accepted_record_counts': counts, 'injected_creation_throttles': int(rejected),
                  'successful_puts_before_throttle': len(before),
                  'passed': passed}
        (out / 'audit.json').write_text(json.dumps(result, indent=2))
        print(json.dumps({'result_directory': str(out), **result}), flush=True)
        if not result['passed']:
            raise RuntimeError('Did not reproduce accepted-record replay; inspect trace')
    finally:
        stopped = subprocess.run(['docker', 'rm', '-f', name], capture_output=True, text=True)
        if process:
            process.wait(timeout=15)
        logfile.close()
        server.shutdown()
        server.server_close()
        temporary.cleanup()
        (out / 'trace.json').write_text(json.dumps(trace, indent=2))
        check = subprocess.run(['docker', 'inspect', name], capture_output=True)
        (out / 'cleanup.json').write_text(json.dumps({'container_absent': check.returncode != 0,
                                                      'server_closed': True, 'aws_resources_created': False}, indent=2))


if __name__ == '__main__':
    main()
