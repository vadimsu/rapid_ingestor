#!/usr/bin/env python3
"""Exercise OTLP request/response and connection cleanup without ClickHouse."""

import json
import re
import socket
import subprocess
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COLLECTOR = ROOT / "build/bin/rapid_ingestor"
GENERATOR = ROOT / "build/bin/otel_traffic_gen"
SEASTAR_OPTIONS = [
    "--smp", "1", "--memory", "256M", "--overprovisioned",
    "--reactor-backend", "epoll",
]


def main():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    with tempfile.TemporaryDirectory() as directory:
        config = Path(directory) / "config.json"
        config.write_text(json.dumps({
            "sources": [{"protocol": "otel", "ip": "127.0.0.1", "port": str(port)}],
            "sinks": [{
                "database_type": "",
                "max_batch_bytes": 65536,
                "backpressure_pause_bytes": 8388608,
                "backpressure_resume_bytes": 4194304,
            }],
        }))
        with (Path(directory) / "collector.log").open("w+") as log:
            collector = subprocess.Popen(
                ["stdbuf", "-oL", "-eL", str(COLLECTOR), "--config", str(config),
                 *SEASTAR_OPTIONS],
                stdout=log, stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 15
                while True:
                    if collector.poll() is not None:
                        raise RuntimeError("collector exited during startup")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                            break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise RuntimeError("collector did not start")
                        time.sleep(0.1)

                base = [str(GENERATOR), "--host", "127.0.0.1", "--port", str(port),
                        *SEASTAR_OPTIONS]
                for signal in ("traces", "metrics", "logs"):
                    subprocess.run(base + ["--signal", signal, "--batches", "100"],
                                   check=True, timeout=30)
                for _ in range(10):
                    subprocess.run(base + ["--batches", "5"], check=True, timeout=30)
                subprocess.run(base + ["--connections", "50", "--batches", "2"],
                               check=True, timeout=30)

                deadline = time.monotonic() + 10
                while True:
                    log.flush()
                    log.seek(0)
                    totals = re.findall(r"Total messages (\d+) bytes \d+ rows inserted \d+ "
                                        r"unparsed \d+ fatal errors (\d+)", log.read())
                    if totals and int(totals[-1][0]) == 450:
                        assert int(totals[-1][1]) == 0, totals[-1]
                        break
                    if collector.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError(f"collector did not report 450 messages: {totals[-1:]}")
                    time.sleep(0.2)
            finally:
                collector.terminate()
                try:
                    collector.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    collector.kill()
                    collector.wait()

        result = subprocess.run(
            base + ["--batches", "1"], capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 or "_instances.empty()" in result.stderr:
            raise RuntimeError(f"connection failure did not exit cleanly: {result.stderr}")
    print("OTLP integration test passed")


if __name__ == "__main__":
    main()
