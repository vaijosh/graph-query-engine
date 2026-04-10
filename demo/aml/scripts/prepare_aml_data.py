#!/usr/bin/env python3
"""
prepare_aml_data.py
-------------------
One-stop script that:
  1. Downloads the IBM AML dataset from Kaggle            (if not already present)
  2. Normalises the raw Kaggle CSV to the engine format   (if not already done)
  3. Seeds the normalised CSV into H2 or Iceberg           (skips if data exists)

All three steps honour MAX_ROWS so loading is always consistent end-to-end:
  • Only MAX_ROWS transaction rows are written to aml-demo.csv
  • Only those rows are loaded into the database

Kaggle dataset
--------------
  ealtman2019/ibm-transactions-for-anti-money-laundering-aml
  https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml

Authentication (one-time setup)
--------------------------------
  Option A — access token (starts with KGAT_, recommended):
    1. Go to https://www.kaggle.com/settings → API → Create New Token
    2. Save the raw token string to  ~/.kaggle/access_token
    3. chmod 600 ~/.kaggle/access_token

  Option B — legacy API key (kaggle.json):
    1. Download kaggle.json from https://www.kaggle.com/settings → API
    2. mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
    3. chmod 600 ~/.kaggle/kaggle.json

  Alternatively export env vars:
    export KAGGLE_USERNAME=<your-username>
    export KAGGLE_KEY=<your-api-key>

Requirements
------------
  pip install kaggle            # Kaggle CLI/SDK (required for download step)
  pip install boto3 trino       # optional — faster Iceberg bulk load

Usage
-----
  # Download from Kaggle + normalize + seed H2  (default)
  python3 demo/aml/scripts/prepare_aml_data.py

  # Use a specific row count (controls all three stages)
  python3 demo/aml/scripts/prepare_aml_data.py --rows 10000

  # Seed Iceberg instead of H2
  python3 demo/aml/scripts/prepare_aml_data.py --backend iceberg

  # Skip download if aml-demo.csv already exists (just seed)
  python3 demo/aml/scripts/prepare_aml_data.py --skip-download

  # Skip download AND normalization (CSV already ready)
  python3 demo/aml/scripts/prepare_aml_data.py --skip-download --skip-normalize

  # Force re-download even if files exist
  python3 demo/aml/scripts/prepare_aml_data.py --force-download

  # Wipe existing DB data and re-seed
  python3 demo/aml/scripts/prepare_aml_data.py --wipe

  # Only download + normalize, don't seed
  python3 demo/aml/scripts/prepare_aml_data.py --no-seed

  # Variant to download (default: HI-Small, smallest file ~30 MB)
  python3 demo/aml/scripts/prepare_aml_data.py --variant HI-Medium
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


# ── Constants ─────────────────────────────────────────────────────────────────

KAGGLE_DATASET = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml"

# Available transaction-file variants ordered by size (smallest first)
VARIANTS = {
    "HI-Small":  "HI-Small_Trans.csv",
    "HI-Medium": "HI-Medium_Trans.csv",
    "HI-Large":  "HI-Large_Trans.csv",
    "LI-Small":  "LI-Small_Trans.csv",
    "LI-Medium": "LI-Medium_Trans.csv",
    "LI-Large":  "LI-Large_Trans.csv",
}

DEFAULT_VARIANT = "HI-Small"
DEFAULT_MAX_ROWS = 100_000


# ── Path helpers ──────────────────────────────────────────────────────────────

def _repo_root() -> Path:
    for p in [Path.cwd()] + list(Path.cwd().parents):
        if (p / "pom.xml").exists():
            return p
    return Path.cwd()


def _data_dir(repo_root: Path) -> Path:
    d = repo_root / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── Step 1: Download from Kaggle ──────────────────────────────────────────────

def _find_kaggle_cmd() -> str | None:
    """Return path to the kaggle CLI, or None if not found."""
    # prefer local venv
    for candidate in [
        Path(sys.executable).parent / "kaggle",
        Path(sys.prefix) / "bin" / "kaggle",
    ]:
        if candidate.exists():
            return str(candidate)
    return shutil.which("kaggle")


def _check_kaggle_credentials() -> None:
    """Raise RuntimeError if no credentials can be found."""
    kaggle_home = Path(os.environ.get("KAGGLE_CONFIG_DIR", Path.home() / ".kaggle"))
    has_env     = os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")
    has_json    = (kaggle_home / "kaggle.json").exists()
    has_token   = (kaggle_home / "access_token").exists()

    if has_env or has_json or has_token:
        return

    raise RuntimeError(
        "\nNo Kaggle credentials found.  Choose one:\n\n"
        "  A) Access token (recommended):\n"
        "       Go to https://www.kaggle.com/settings → API → 'Create New Token'\n"
        f"       Save the token string to: {kaggle_home / 'access_token'}\n"
        f"       chmod 600 {kaggle_home / 'access_token'}\n\n"
        "  B) Legacy API key (kaggle.json):\n"
        "       Download kaggle.json from https://www.kaggle.com/settings → API\n"
        f"       mv ~/Downloads/kaggle.json {kaggle_home / 'kaggle.json'}\n"
        f"       chmod 600 {kaggle_home / 'kaggle.json'}\n\n"
        "  C) Environment variables:\n"
        "       export KAGGLE_USERNAME=<your-username>\n"
        "       export KAGGLE_KEY=<your-api-key>\n\n"
        "After setting credentials, re-run this script.\n"
        "You may also need to accept the dataset rules at:\n"
        "  https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml\n"
        "(click 'Rules' → 'I Understand and Accept')"
    )


def download_from_kaggle(
    variant: str,
    dest_dir: Path,
    force: bool = False,
) -> Path:
    """
    Download the transaction CSV for *variant* from Kaggle into *dest_dir*.
    Returns the path to the downloaded file.

    Uses the kaggle Python SDK if available, otherwise falls back to the
    kaggle CLI subprocess.
    """
    trans_file = VARIANTS[variant]
    dest_path  = dest_dir / trans_file

    if dest_path.exists() and not force:
        print(f"  ✓ Already downloaded: {dest_path}")
        return dest_path

    print(f"  Downloading {variant} from Kaggle dataset '{KAGGLE_DATASET}' …")
    print(f"  Target file : {trans_file}")
    print(f"  Destination : {dest_dir}")

    _check_kaggle_credentials()

    # Try kaggle Python SDK first (cleaner, no subprocess)
    try:
        import kaggle  # type: ignore  # noqa: F401
        from kaggle.api.kaggle_api_extended import KaggleApiExtended  # type: ignore
        api = KaggleApiExtended()
        api.authenticate()
        print("  Using Kaggle Python SDK …")
        with tempfile.TemporaryDirectory() as tmpdir:
            api.dataset_download_file(
                KAGGLE_DATASET,
                file_name=trans_file,
                path=tmpdir,
                quiet=False,
            )
            # The SDK may download a .zip — unzip if needed
            zip_path = Path(tmpdir) / (trans_file + ".zip")
            if zip_path.exists():
                with zipfile.ZipFile(zip_path) as zf:
                    zf.extractall(tmpdir)
            extracted = Path(tmpdir) / trans_file
            if not extracted.exists():
                # try without subdirectory
                candidates = list(Path(tmpdir).rglob(trans_file))
                if candidates:
                    extracted = candidates[0]
                else:
                    raise FileNotFoundError(
                        f"Expected {trans_file} in download but not found in {tmpdir}"
                    )
            shutil.move(str(extracted), str(dest_path))
        print(f"  ✓ Downloaded → {dest_path}")
        return dest_path

    except ImportError:
        pass  # fall through to CLI

    # Fallback: kaggle CLI
    kaggle_cmd = _find_kaggle_cmd()
    if not kaggle_cmd:
        raise RuntimeError(
            "kaggle package not found. Install it with:\n"
            "  pip install kaggle\n"
            "Then set up credentials (see --help for details)."
        )

    print(f"  Using kaggle CLI: {kaggle_cmd}")
    with tempfile.TemporaryDirectory() as tmpdir:
        result = subprocess.run(
            [kaggle_cmd, "datasets", "download",
             "--dataset", KAGGLE_DATASET,
             "--file", trans_file,
             "--path", tmpdir,
             "--unzip"],
            capture_output=False,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"kaggle download failed (exit {result.returncode}).\n"
                "Make sure you have accepted the dataset rules at:\n"
                "  https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
            )
        candidates = list(Path(tmpdir).rglob(trans_file))
        if not candidates:
            raise FileNotFoundError(
                f"Download succeeded but {trans_file} not found in {tmpdir}"
            )
        shutil.move(str(candidates[0]), str(dest_path))

    print(f"  ✓ Downloaded → {dest_path}")
    return dest_path


# ── Step 2: Normalise raw Kaggle CSV → engine format ─────────────────────────

# Kaggle AML schema (positional, header row present):
# 0:Timestamp  1:From Bank  2:Account(from)  3:To Bank  4:Account(to)
# 5:Amount Received  6:Receiving Currency  7:Amount Paid
# 8:Payment Currency  9:Payment Format  10:Is Laundering
_ENGINE_HEADER = (
    "from_bank,from_account,to_bank,to_account,"
    "amount_paid,payment_currency,payment_format,"
    "timestamp,is_laundering,transaction_id\n"
)


def normalize_csv(src: Path, dst: Path, max_rows: int) -> Path:
    """
    Convert the raw Kaggle transaction CSV to the engine's expected format.
    Writes exactly *max_rows* rows (or fewer if the source is smaller).
    Returns *dst*.
    """
    if dst.exists():
        # Check whether the existing file was built with the same row limit
        with open(dst, newline="", encoding="utf-8") as f:
            existing_rows = sum(1 for _ in f) - 1  # subtract header
        if existing_rows >= max_rows:
            print(f"  ✓ Normalised CSV already exists ({existing_rows:,} rows): {dst}")
            return dst
        print(f"  Existing normalised CSV has {existing_rows:,} rows < {max_rows:,} — re-normalising …")

    print(f"  Normalising {src.name} → {dst.name}  (max {max_rows:,} rows) …")
    count = 0
    laundering = 0

    with src.open(newline="", encoding="utf-8", errors="ignore") as fin, \
         dst.open("w", newline="", encoding="utf-8") as fout:

        reader = csv.reader(fin)
        _raw_headers = next(reader)  # skip original header
        fout.write(_ENGINE_HEADER)

        for i, row in enumerate(reader):
            if count >= max_rows:
                break
            if len(row) < 11:
                continue
            from_bank    = row[1].strip()
            from_account = row[2].strip()
            to_bank      = row[3].strip()
            to_account   = row[4].strip()
            amount_paid  = row[7].strip()
            pay_currency = row[8].strip()
            pay_format   = (row[9].strip() or "UNKNOWN")
            timestamp    = row[0].strip()
            is_launder   = row[10].strip()
            tx_id        = str(i + 1)

            fout.write(
                f"{from_bank},{from_account},{to_bank},{to_account},"
                f"{amount_paid},{pay_currency},{pay_format},"
                f"{timestamp},{is_launder},{tx_id}\n"
            )
            if is_launder == "1":
                laundering += 1
            count += 1

    print(f"  ✓ Wrote {count:,} rows ({laundering:,} suspicious) → {dst}")
    return dst


# ── Step 3: Seed database ─────────────────────────────────────────────────────

def seed_h2(csv_path: Path, max_rows: int, wipe: bool, repo_root: Path) -> None:
    seed_script = repo_root / "demo" / "aml" / "scripts" / "seed_aml_h2.py"
    if not seed_script.exists():
        raise FileNotFoundError(f"Seed script not found: {seed_script}")

    cmd = [
        sys.executable, str(seed_script),
        "--csv", str(csv_path),
        "--max-rows", str(max_rows),
        "--mode", "csvread",
    ]
    if wipe:
        cmd.append("--wipe")

    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(repo_root))
    if result.returncode != 0:
        raise RuntimeError("H2 seed failed — see output above.")


def seed_iceberg(csv_path: Path, max_rows: int, wipe: bool, repo_root: Path) -> None:
    seed_script = repo_root / "demo" / "infra" / "scripts" / "seed_iceberg_from_csv.py"
    if not seed_script.exists():
        raise FileNotFoundError(f"Seed script not found: {seed_script}")

    cmd = [
        sys.executable, str(seed_script),
        "--dataset", "aml",
        "--csv", str(csv_path),
        "--rows", str(max_rows),
    ]
    if wipe:
        cmd.append("--wipe")

    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(repo_root))
    if result.returncode != 0:
        raise RuntimeError("Iceberg seed failed — see output above.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download AML dataset from Kaggle, normalise, and seed the graph engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--rows", type=int, default=DEFAULT_MAX_ROWS,
        help=f"Number of transaction rows to download/normalise/load (default: {DEFAULT_MAX_ROWS:,}). "
             "Controls ALL three stages — only this many rows end up in the database.",
    )
    parser.add_argument(
        "--variant", choices=list(VARIANTS.keys()), default=DEFAULT_VARIANT,
        help=f"Kaggle dataset variant to download (default: {DEFAULT_VARIANT}). "
             "Variants ordered smallest → largest: HI-Small (~30 MB) … HI-Large (~1 GB).",
    )
    parser.add_argument(
        "--backend", choices=["h2", "iceberg"], default="h2",
        help="Seed target: 'h2' (default, no Docker needed) or 'iceberg' (Docker required).",
    )
    parser.add_argument(
        "--skip-download", action="store_true",
        help="Skip Kaggle download. Uses the existing raw file in data/ (or the local aml-demo.csv).",
    )
    parser.add_argument(
        "--skip-normalize", action="store_true",
        help="Skip normalisation. Uses data/aml-demo.csv as-is. Implies --skip-download.",
    )
    parser.add_argument(
        "--force-download", action="store_true",
        help="Re-download from Kaggle even if the file already exists locally.",
    )
    parser.add_argument(
        "--wipe", action="store_true",
        help="Delete existing data from the target database before seeding.",
    )
    parser.add_argument(
        "--no-seed", action="store_true",
        help="Only download + normalise; skip the database seed step.",
    )
    parser.add_argument(
        "--csv", default=None,
        help="Path to an already-normalised engine-format CSV to use directly "
             "(skips download + normalisation).",
    )
    args = parser.parse_args()

    repo_root = _repo_root()
    data_dir  = _data_dir(repo_root)

    print(f"\n{'='*60}")
    print(f"  AML Data Preparation")
    print(f"  rows    : {args.rows:,}")
    print(f"  variant : {args.variant}")
    print(f"  backend : {args.backend}")
    print(f"  repo    : {repo_root}")
    print(f"{'='*60}\n")

    # ── Resolve CSV path ──────────────────────────────────────────────────────
    if args.csv:
        csv_path = Path(args.csv)
        if not csv_path.exists():
            print(f"ERROR: --csv file not found: {csv_path}", file=sys.stderr)
            sys.exit(1)
        print(f"Step 0  Using provided CSV: {csv_path}\n")
    else:
        normalised_csv = data_dir / "aml-demo.csv"

        # Step 1 — Download
        if args.skip_normalize or args.skip_download:
            print("Step 1  Download — SKIPPED")
            raw_csv = data_dir / VARIANTS[args.variant]
        else:
            print("Step 1  Download from Kaggle")
            raw_csv = download_from_kaggle(
                args.variant, data_dir, force=args.force_download
            )
            print()

        # Step 2 — Normalise
        if args.skip_normalize:
            if not normalised_csv.exists():
                print(f"ERROR: --skip-normalize requested but {normalised_csv} does not exist.",
                      file=sys.stderr)
                sys.exit(1)
            print(f"Step 2  Normalise — SKIPPED (using {normalised_csv})")
            csv_path = normalised_csv
        else:
            print("Step 2  Normalise")
            csv_path = normalize_csv(raw_csv, normalised_csv, args.rows)
            print()

    # ── Step 3 — Seed ─────────────────────────────────────────────────────────
    if args.no_seed:
        print("Step 3  Seed — SKIPPED (--no-seed)")
    else:
        print(f"Step 3  Seed → {args.backend.upper()}")
        if args.backend == "h2":
            seed_h2(csv_path, args.rows, args.wipe, repo_root)
        else:
            seed_iceberg(csv_path, args.rows, args.wipe, repo_root)
        print()

    print(f"\n{'='*60}")
    print(f"  Done!  CSV: {csv_path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

