from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import threading
import unittest


SCRIPT = Path(__file__).parents[1] / "relay-demo.sh"


def sourced(command: str, **environment: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", f'source "$1"; {command}', "bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, **environment},
    )


class PrometheusHandler(BaseHTTPRequestHandler):
    status = 200
    body = b""

    def do_GET(self):
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, format, *args):
        pass


class PrometheusStub:
    def __init__(self, status: int, body: dict | str):
        PrometheusHandler.status = status
        PrometheusHandler.body = (
            json.dumps(body).encode() if isinstance(body, dict) else body.encode()
        )
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), PrometheusHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self.server.server_port

    def __exit__(self, exc_type, exc_value, traceback):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class RelayDemoOutcomeTest(unittest.TestCase):
    def test_scaling_requires_backlog_growth_scale_out_drain_and_scale_in(self):
        passed = sourced("scale_succeeded 1 0 1 598 12")

        self.assertEqual(passed.returncode, 0, passed.stderr)

        failures = {
            "sink was never released": "scale_succeeded 0 0 1 598 12",
            "lag never rose": "scale_succeeded 1 0 1 0 12",
            "consumers never scaled": "scale_succeeded 1 0 1 598 1",
            "lag did not drain": "scale_succeeded 1 4 1 598 12",
            "consumer metrics disappeared": "scale_succeeded 1 0 0 598 12",
            "consumers did not scale in": "scale_succeeded 1 0 2 598 12",
        }
        for name, command in failures.items():
            with self.subTest(name=name):
                result = sourced(command)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stderr, "")

    def test_subscriber_outcome_requires_fresh_delivery_and_dead_letter(self):
        passed = sourced("dlq_succeeded 7 8 40 41")

        self.assertEqual(passed.returncode, 0, passed.stderr)

        failures = {
            "old counters": "dlq_succeeded 7 7 40 40",
            "dead letter only": "dlq_succeeded 7 8 40 40",
            "delivery only": "dlq_succeeded 7 7 40 41",
        }
        for name, command in failures.items():
            with self.subTest(name=name):
                result = sourced(command)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stderr, "")

    def test_prometheus_query_returns_one_numeric_sample(self):
        response = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {}, "value": [0, "7"]}],
            },
        }
        with PrometheusStub(200, response) as port:
            result = sourced("promq 'sum(example_total)'", DEMO_PROM_PORT=str(port))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "7\n")

    def test_prometheus_query_rejects_transport_and_response_failures(self):
        empty = {
            "status": "success",
            "data": {"resultType": "vector", "result": []},
        }
        cases = {
            "http error": (503, "unavailable"),
            "malformed json": (200, "{"),
            "empty vector": (200, empty),
        }
        for name, (status, body) in cases.items():
            with self.subTest(name=name), PrometheusStub(status, body) as port:
                result = sourced(
                    "promq 'sum(example_total)'", DEMO_PROM_PORT=str(port)
                )
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
