#!/usr/bin/env zsh
set -euo pipefail

# Run from repo root regardless of current location.
SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

echo "[1/3] Removing IntelliJ module file"
rm -f "GraphQueryEngine.iml"

echo "[2/3] Refreshing Maven dependencies and running tests"
mvn -U clean test

echo "[3/3] Done"
echo "Now open IntelliJ and reload Maven projects from pom.xml"

