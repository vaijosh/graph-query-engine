#!/usr/bin/env bash
# =============================================================================
# download_aml_data.sh — Download the IBM AML (Anti-Money Laundering) dataset
#                        from Kaggle and extract it into demo/data/
#
# Dataset: IBM Transactions for Anti Money Laundering (AML)
# Kaggle:  https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
#
# Requirements
#   • Kaggle CLI:  pip install kaggle
#   • API token:   ~/.kaggle/kaggle.json  (create at kaggle.com → Settings → API)
#
# Usage
#   chmod +x scripts/download_aml_data.sh
#   ./scripts/download_aml_data.sh              # download all variants
#   ./scripts/download_aml_data.sh --variant HI-Small   # single variant
#   ./scripts/download_aml_data.sh --skip-normalize     # skip CSV normalization step
#   ./scripts/download_aml_data.sh --rows 50000         # normalize with 50k rows
#
# After the script completes the following files are available in demo/data/:
#   HI-Small_Trans.csv, HI-Small_accounts.csv, HI-Small_Patterns.txt
#   HI-Medium_Trans.csv, ...  (if --variant not specified)
#   HI-Large_Trans.csv,  ...
#   LI-Small_Trans.csv,  ...
#   LI-Medium_Trans.csv, ...
#   LI-Large_Trans.csv,  ...
#   aml-demo.csv         (normalized, ready for loader)
#   aml-normalized.csv   (full normalized output)
# =============================================================================

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
DATASET="ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
DEST_DIR="$(cd "$(dirname "$0")/.." && pwd)/demo/data"
VARIANT=""          # empty = download all
SKIP_NORMALIZE=0
NORMALIZE_ROWS=100000
NORMALIZE_SRC="HI-Small_Trans.csv"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --variant)       VARIANT="$2"; shift 2 ;;
        --skip-normalize) SKIP_NORMALIZE=1; shift ;;
        --rows)          NORMALIZE_ROWS="$2"; shift 2 ;;
        --src)           NORMALIZE_SRC="$2"; shift 2 ;;
        --dest)          DEST_DIR="$2"; shift 2 ;;
        -h|--help)
            sed -n '/^# Usage/,/^# =\{10\}/p' "$0" | head -20
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
info()  { echo "[INFO]  $*"; }
warn()  { echo "[WARN]  $*" >&2; }
error() { echo "[ERROR] $*" >&2; exit 1; }

# ── Pre-flight checks ─────────────────────────────────────────────────────────
info "Checking prerequisites..."

if ! command -v kaggle &>/dev/null; then
    error "kaggle CLI not found. Install it with: pip install kaggle
       Then create your API token at https://www.kaggle.com/settings
       and save it to ~/.kaggle/kaggle.json"
fi

KAGGLE_JSON="${KAGGLE_CONFIG_DIR:-$HOME/.kaggle}/kaggle.json"
if [[ ! -f "$KAGGLE_JSON" ]]; then
    error "Kaggle API token not found at $KAGGLE_JSON
       Create one at https://www.kaggle.com/settings → API → Create New Token"
fi

chmod 600 "$KAGGLE_JSON"

# ── Create destination directory ──────────────────────────────────────────────
mkdir -p "$DEST_DIR"
info "Destination: $DEST_DIR"

# ── Download ──────────────────────────────────────────────────────────────────
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

if [[ -n "$VARIANT" ]]; then
    # Download only the files matching the chosen variant (e.g. HI-Small)
    info "Downloading variant '$VARIANT' from Kaggle dataset '$DATASET'..."
    FILES=(
        "${VARIANT}_Trans.csv"
        "${VARIANT}_accounts.csv"
        "${VARIANT}_Patterns.txt"
    )
    for f in "${FILES[@]}"; do
        info "  → $f"
        kaggle datasets download \
            --dataset "$DATASET" \
            --file "$f" \
            --path "$TMP_DIR" \
            --unzip 2>&1 | sed 's/^/    /'
    done
else
    info "Downloading full dataset from Kaggle (this may take a while)..."
    kaggle datasets download \
        --dataset "$DATASET" \
        --path "$TMP_DIR" \
        --unzip 2>&1 | sed 's/^/    /'
fi

# ── Move extracted files into demo/data/ ─────────────────────────────────────
info "Moving files to $DEST_DIR ..."
shopt -s nullglob
csv_files=("$TMP_DIR"/*.csv "$TMP_DIR"/**/*.csv)
txt_files=("$TMP_DIR"/*.txt "$TMP_DIR"/**/*.txt)

moved=0
for f in "${csv_files[@]}" "${txt_files[@]}"; do
    fname=$(basename "$f")
    # Only move IBM AML files (HI-* or LI-* patterns)
    if [[ "$fname" =~ ^(HI|LI)- ]]; then
        mv -f "$f" "$DEST_DIR/$fname"
        info "  ✓ $fname"
        ((moved++)) || true
    fi
done

if [[ $moved -eq 0 ]]; then
    warn "No HI-*/LI-* files found in the downloaded archive."
    warn "Files in tmp: $(ls "$TMP_DIR" 2>/dev/null | head -10)"
    error "Download may have failed or the dataset structure changed."
fi

info "Downloaded $moved file(s) to $DEST_DIR"

# ── Normalise to loader format ────────────────────────────────────────────────
if [[ $SKIP_NORMALIZE -eq 0 ]]; then
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    NORMALIZE_PY="$SCRIPT_DIR/normalize_aml.py"

    if [[ ! -f "$NORMALIZE_PY" ]]; then
        warn "normalize_aml.py not found at $NORMALIZE_PY — skipping normalization."
    else
        SRC_FILE="$DEST_DIR/$NORMALIZE_SRC"
        if [[ ! -f "$SRC_FILE" ]]; then
            # Fall back to any available HI-*_Trans.csv
            SRC_FILE=$(ls "$DEST_DIR"/HI-*_Trans.csv 2>/dev/null | sort | head -1 || true)
        fi

        if [[ -z "$SRC_FILE" || ! -f "$SRC_FILE" ]]; then
            warn "No source transaction CSV found for normalization — skipping."
        else
            info "Normalizing $SRC_FILE → demo/data/aml-demo.csv (${NORMALIZE_ROWS} rows)..."
            python3 "$NORMALIZE_PY" \
                --src "$SRC_FILE" \
                --dst "$DEST_DIR/aml-demo.csv" \
                --rows "$NORMALIZE_ROWS"
        fi
    fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo " AML data ready in: $DEST_DIR"
echo "============================================================"
ls -lh "$DEST_DIR" | grep -E '\.(csv|txt)$' || true
echo ""
echo "Next steps:"
echo "  1. Start the engine:      mvn exec:java"
echo "  2. Load into the engine:"
echo "     curl -X POST \"http://localhost:7000/admin/load-aml-csv?path=\$(pwd)/demo/data/aml-demo.csv&maxRows=${NORMALIZE_ROWS}\""
echo "  3. Open the notebook:     jupyter notebook aml_demo_queries.ipynb"
echo ""

