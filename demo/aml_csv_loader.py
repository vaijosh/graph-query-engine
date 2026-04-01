"""
aml_csv_loader.py
-----------------
Loads an AML transaction CSV file into the running Graph Query Engine by
calling the /admin/load-aml-csv and /mapping/upload HTTP endpoints.

Usage (from notebook):
    from demo.aml_csv_loader import AmlCsvLoader
    stats = AmlCsvLoader(base_url="http://localhost:7000").load(
        csv_path="demo/data/aml-demo.csv", max_rows=100_000
    )
    print(stats)

Usage (standalone):
    python3 demo/aml_csv_loader.py --path demo/data/aml-demo.csv --max-rows 100000
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

import requests


class AmlCsvLoader:
    """
    Loads an AML transaction CSV into the Graph Query Engine's in-memory
    TinkerGraph by calling the /admin/load-aml-csv endpoint.
    """

    def __init__(self, base_url: str = "http://localhost:7000", timeout: int = 300):
        """
        Parameters
        ----------
        base_url : str
            Base URL of the running Graph Query Engine service.
        timeout : int
            HTTP request timeout in seconds (default 300 — large CSVs take time).
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        csv_path: str | Path,
        max_rows: int = 100_000,
        *,
        verbose: bool = True,
    ) -> dict[str, Any]:
        """
        Load *csv_path* into the engine's TinkerGraph.

        Parameters
        ----------
        csv_path : str | Path
            Absolute or relative path to the AML CSV file.
            The path is resolved to an absolute path so the Java backend
            (which may have a different working directory) can find it.
        max_rows : int
            Maximum number of transaction rows to load.
        verbose : bool
            Print progress messages.

        Returns
        -------
        dict
            Load statistics from the server:
            ``rowsLoaded``, ``accountsCreated``, ``transfersCreated``,
            ``sourcePath``, ``maxRows``, ``provider``.

        Raises
        ------
        FileNotFoundError
            If *csv_path* does not exist locally.
        RuntimeError
            If the server returns an error response.
        """
        resolved = Path(csv_path).resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"CSV file not found: {resolved}")

        if verbose:
            size_mb = resolved.stat().st_size / (1024 * 1024)
            print(f"📂  Loading CSV  : {resolved}")
            print(f"📏  File size    : {size_mb:.1f} MB")
            print(f"🔢  Max rows     : {max_rows:,}")
            print(f"🌐  Endpoint     : {self.base_url}/admin/load-aml-csv")

        url = f"{self.base_url}/admin/load-aml-csv"
        params = {"path": str(resolved), "maxRows": str(max_rows)}

        t0 = time.time()
        try:
            response = requests.post(url, params=params, timeout=self.timeout)
        except requests.exceptions.ConnectionError as exc:
            raise RuntimeError(
                f"Could not connect to Graph Query Engine at {self.base_url}.\n"
                f"Make sure the service is running (e.g. 'mvn exec:java' or "
                f"'python3 demo/demo_app.py').\nOriginal error: {exc}"
            ) from exc

        elapsed = time.time() - t0

        if not response.ok:
            body = response.text or "<empty response>"
            raise RuntimeError(
                f"Server returned HTTP {response.status_code} for "
                f"POST /admin/load-aml-csv:\n{body}"
            )

        stats: dict[str, Any] = response.json()

        if "error" in stats:
            raise RuntimeError(f"Server error: {stats['error']}")

        if verbose:
            print(f"\n✅  Load complete ({elapsed:.1f}s)")
            print(f"   Rows loaded      : {stats.get('rowsLoaded', '?'):,}")
            print(f"   Accounts created : {stats.get('accountsCreated', '?'):,}")
            print(f"   Transfers created: {stats.get('transfersCreated', '?'):,}")
            print(f"   Provider         : {stats.get('provider', '?')}")

        return stats

    # ------------------------------------------------------------------
    # Convenience: also upload the AML mapping if not already active
    # ------------------------------------------------------------------

    def upload_mapping(
        self,
        mapping_path: str | Path,
        mapping_id: str = "aml",
        mapping_name: str = "AML Mapping",
        activate: bool = True,
        *,
        verbose: bool = True,
    ) -> dict[str, Any]:
        """
        Upload a JSON mapping file to the engine and optionally activate it.

        Parameters
        ----------
        mapping_path : str | Path
            Path to the mapping JSON file (e.g. ``demo/mappings/aml-mapping.json``).
        mapping_id : str
            Stable ID for the mapping (used for idempotent re-uploads).
        mapping_name : str
            Human-readable label shown in the UI.
        activate : bool
            Whether to make this mapping the active one immediately.
        verbose : bool
            Print progress messages.
        """
        resolved = Path(mapping_path).resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"Mapping file not found: {resolved}")

        if verbose:
            print(f"📋  Uploading mapping: {resolved.name}  (id={mapping_id})")

        url = f"{self.base_url}/mapping/upload"
        params = {"id": mapping_id, "name": mapping_name, "activate": str(activate).lower()}

        with resolved.open("rb") as f:
            response = requests.post(
                url,
                params=params,
                files={"file": (resolved.name, f, "application/json")},
                timeout=30,
            )

        if not response.ok:
            raise RuntimeError(
                f"Mapping upload failed (HTTP {response.status_code}): {response.text}"
            )

        result: dict[str, Any] = response.json()
        if "error" in result:
            raise RuntimeError(f"Mapping upload error: {result['error']}")

        if verbose:
            print(f"✅  Mapping uploaded — id={result.get('mappingId')}, "
                  f"active={result.get('active')}")

        return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Load an AML CSV into the Graph Query Engine."
    )
    parser.add_argument(
        "--path",
        default="demo/data/aml-demo.csv",
        help="Path to the AML CSV file (default: demo/data/aml-demo.csv)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100_000,
        dest="max_rows",
        help="Maximum rows to load (default: 100000)",
    )
    parser.add_argument(
        "--url",
        default="http://localhost:7000",
        help="Base URL of the engine service (default: http://localhost:7000)",
    )
    parser.add_argument(
        "--mapping",
        default=None,
        help="Optional path to a mapping JSON to upload before loading",
    )
    args = parser.parse_args()

    loader = AmlCsvLoader(base_url=args.url)
    if args.mapping:
        loader.upload_mapping(args.mapping)
    loader.load(csv_path=args.path, max_rows=args.max_rows)


if __name__ == "__main__":
    _main()

