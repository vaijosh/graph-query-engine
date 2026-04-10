"""
demo_app.py
-----------

Starts the Graph Query Engine as a background subprocess, waits until it is
healthy, and optionally seeds it with an AML CSV — all from Python so a
Jupyter notebook can do the full setup in a single cell.

Usage (from notebook):
    from demo.demo_app import DemoApp
    app = DemoApp()
    app.start()                     # starts the Java service (H2 by default)
    app.load_csv()                  # loads default AML CSV + mapping
    # ... run queries ...
    app.stop()                      # optional: stops the service on shutdown

Multi-backend usage (H2 + Iceberg in the same process):
    app = DemoApp(backends=[
        {"id": "h2",      "url": "jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE"},
        {"id": "iceberg", "url": "jdbc:trino://localhost:8080/iceberg/aml",
                          "user": "admin", "password": ""},
    ])
    app.start()
    # Queries now select backend via X-Backend-Id header:
    #   requests.post("/gremlin/query", json={...}, headers={"X-Backend-Id": "iceberg"})

Usage (as a context manager):
    with DemoApp() as app:
        app.load_csv()
        # ... run queries ...

Usage (standalone):
    python3 demo/demo_app.py
    python3 demo/demo_app.py --csv demo/data/aml-demo.csv --max-rows 50000
    python3 demo/demo_app.py --backends '[{"id":"h2","url":"jdbc:h2:file:./data/graph"},{"id":"iceberg","url":"jdbc:trino://localhost:8080/iceberg/aml","user":"admin"}]'
"""

from __future__ import annotations

import argparse
import json as _json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

from demo.aml_csv_loader import AmlCsvLoader

# Repository root = parent of the directory that contains this file
_REPO_ROOT = Path(__file__).resolve().parent.parent


class DemoApp:
    """
    Manages the lifecycle of the Graph Query Engine service for demo/notebook use.

    Starts the engine as a subprocess, waits for it to become healthy,
    and exposes CSV loading and mapping upload via :class:`AmlCsvLoader`.

    Parameters
    ----------
    base_url : str
        URL the service will listen on.
    port : int
        Port passed to the Java service via the ``PORT`` env var.
    startup_timeout : int
        Seconds to wait for the service to become healthy after start.
    repo_root : Path | None
        Root directory of the project (auto-detected from this file's location).
    backends : list[dict] | None
        Optional list of backend configs to pass as the ``BACKENDS`` JSON env var.
        Each dict should have at minimum ``id`` and ``url`` keys; ``user`` and
        ``password`` are optional.  When supplied, ``DB_URL``/``DB_DRIVER`` are
        *not* forwarded to the subprocess so the ``BACKENDS`` list is the sole
        source of truth.

        Example::

            backends=[
                {"id": "h2",      "url": "jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE"},
                {"id": "iceberg", "url": "jdbc:trino://localhost:8080/iceberg/aml",
                                  "user": "admin"},
            ]
    """

    DEFAULT_CSV_PATH     = "demo/data/aml-demo.csv"
    DEFAULT_MAPPING_PATH = "demo/aml/mappings/aml-mapping.json"
    DEFAULT_MAX_ROWS     = 100_000

    def __init__(
        self,
        base_url: str = "http://localhost:7000",
        port: int = 7000,
        startup_timeout: int = 60,
        repo_root: Path | None = None,
        backends: list[dict] | None = None,
    ):
        self.base_url        = base_url.rstrip("/")
        self.port            = port
        self.startup_timeout = startup_timeout
        self.repo_root       = repo_root or _REPO_ROOT
        self.backends        = backends
        self._process: subprocess.Popen | None = None
        self._loader = AmlCsvLoader(base_url=self.base_url)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, *, skip_if_running: bool = True) -> "DemoApp":
        """
        Start the Graph Query Engine service as a subprocess.

        Parameters
        ----------
        skip_if_running : bool
            If the service is already reachable, skip launching a new process.
        """
        if skip_if_running and self._is_healthy():
            print(f"✅  Service already running at {self.base_url}")
            return self

        print(f"🚀  Starting Graph Query Engine on port {self.port} …")
        env = {**os.environ, "PORT": str(self.port)}

        # Inject multi-backend config when provided.
        if self.backends:
            env["BACKENDS"] = _json.dumps(self.backends)
            # Remove legacy single-backend vars so they don't conflict.
            for key in ("DB_URL", "DB_USER", "DB_PASSWORD", "DB_DRIVER"):
                env.pop(key, None)
            backend_ids = ", ".join(b.get("id", "?") for b in self.backends)
            print(f"   Multi-backend mode: [{backend_ids}]")

        # Prefer the pre-built fat-jar at the root target/; otherwise fall back to mvn exec:java
        engine_jar = self.repo_root / "target" / "graph-query-engine-0.1.0.jar"

        if engine_jar.exists():
            cmd = [
                "java", "-cp", str(engine_jar),
                "com.graphqueryengine.App",
            ]
            print(f"   Using jar: {engine_jar.name}")
        else:
            # Fall back to Maven exec (root pom.xml is now a flat jar project)
            cmd = ["mvn", "-f", str(self.repo_root / "pom.xml"), "exec:java"]
            print("   Using: mvn exec:java")

        self._process = subprocess.Popen(
            cmd,
            cwd=str(self.repo_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        self._wait_for_health()
        return self

    def stop(self) -> None:
        """Stop the service subprocess if one was started by this instance."""
        if self._process is None:
            return
        print("🛑  Stopping Graph Query Engine …")
        self._process.send_signal(signal.SIGTERM)
        try:
            self._process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._process.kill()
        self._process = None
        print("   Service stopped.")

    def is_running(self) -> bool:
        """Return True if the service is reachable and healthy."""
        return self._is_healthy()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_csv(
        self,
        csv_path: str | Path | None = None,
        max_rows: int | None = None,
        mapping_path: str | Path | None = None,
        *,
        verbose: bool = True,
    ) -> dict[str, Any]:
        """
        Upload the AML mapping and load the CSV into the engine.

        Parameters
        ----------
        csv_path : str | Path | None
            Path to the AML CSV. Defaults to ``demo/data/aml-demo.csv``.
        max_rows : int | None
            Maximum rows to load. Defaults to 100,000.
        mapping_path : str | Path | None
            Path to the mapping JSON. Defaults to ``demo/aml/mappings/aml-mapping.json``.
        verbose : bool
            Print progress messages.

        Returns
        -------
        dict
            Load statistics (rowsLoaded, accountsCreated, transfersCreated, …).
        """
        csv_resolved     = self.repo_root / (csv_path     or self.DEFAULT_CSV_PATH)
        mapping_resolved = self.repo_root / (mapping_path or self.DEFAULT_MAPPING_PATH)
        rows             = max_rows if max_rows is not None else self.DEFAULT_MAX_ROWS

        self._loader.upload_mapping(mapping_resolved, verbose=verbose)
        return self._loader.load(csv_resolved, rows, verbose=verbose)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "DemoApp":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_healthy(self) -> bool:
        try:
            r = requests.get(f"{self.base_url}/health", timeout=3)
            return r.ok
        except Exception:
            return False

    def _wait_for_health(self) -> None:
        deadline = time.time() + self.startup_timeout
        dots = 0
        while time.time() < deadline:
            if self._process and self._process.poll() is not None:
                out, _ = self._process.communicate()
                raise RuntimeError(
                    f"Service exited unexpectedly (code {self._process.returncode}).\n"
                    + (out.decode(errors="replace") if out else "")
                )
            if self._is_healthy():
                print(f"\n✅  Service is healthy at {self.base_url}")
                return
            print("." if dots % 40 else "\n   Waiting", end="", flush=True)
            dots += 1
            time.sleep(1)
        raise TimeoutError(
            f"Service did not become healthy within {self.startup_timeout}s. "
            f"Check that port {self.port} is not already in use."
        )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Start the Graph Query Engine and load AML demo data."
    )
    parser.add_argument("--port",     type=int, default=7000)
    parser.add_argument("--csv",      default=DemoApp.DEFAULT_CSV_PATH)
    parser.add_argument("--max-rows", type=int, default=DemoApp.DEFAULT_MAX_ROWS, dest="max_rows")
    parser.add_argument("--mapping",  default=DemoApp.DEFAULT_MAPPING_PATH)
    parser.add_argument(
        "--no-load", action="store_true", dest="no_load",
        help="Start the service but skip CSV loading",
    )
    parser.add_argument(
        "--backends", default=None,
        help=(
            "JSON array of backend configs, e.g. "
            "'[{\"id\":\"h2\",\"url\":\"jdbc:h2:file:./data/graph\"},"
            "{\"id\":\"iceberg\",\"url\":\"jdbc:trino://localhost:8080/iceberg/aml\",\"user\":\"admin\"}]'"
        ),
    )
    args = parser.parse_args()

    backends = None
    if args.backends:
        try:
            backends = _json.loads(args.backends)
        except _json.JSONDecodeError as e:
            print(f"ERROR: --backends is not valid JSON: {e}", file=sys.stderr)
            sys.exit(1)

    app = DemoApp(port=args.port, backends=backends)
    try:
        app.start()
        if not args.no_load:
            app.load_csv(args.csv, args.max_rows, args.mapping)
        print("\n🎯  Demo ready. Press Ctrl+C to stop.")
        signal.pause()
    except KeyboardInterrupt:
        pass
    finally:
        app.stop()


if __name__ == "__main__":
    _main()
