"""
aml_csv_loader.py
-----------------
Loads an AML transaction CSV file into the running Graph Query Engine by
calling /gremlin/query (mutation script) and /mapping/upload HTTP endpoints.

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
    TinkerGraph by executing a Gremlin-Groovy mutation script via /gremlin/query.
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
            print(f"🌐  Endpoint     : {self.base_url}/gremlin/query")

        url = f"{self.base_url}/gremlin/query"
        gremlin_script = self._build_loader_script(str(resolved), max_rows)
        payload = {"gremlin": gremlin_script}

        t0 = time.time()
        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
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
                f"POST /gremlin/query (AML load script):\n{body}"
            )

        response_json: dict[str, Any] = response.json()
        if "error" in response_json:
            raise RuntimeError(f"Server error: {response_json['error']}")

        results = response_json.get("results") or []
        if not results or not isinstance(results[0], dict):
            raise RuntimeError(
                "Unexpected load-script response shape from /gremlin/query; "
                f"got: {response_json}"
            )
        stats = dict(results[0])

        if verbose:
            print(f"\n✅  Load complete ({elapsed:.1f}s)")
            print(f"   Rows loaded      : {stats.get('rowsLoaded', '?'):,}")
            print(f"   Accounts created : {stats.get('accountsCreated', '?'):,}")
            print(f"   Transfers created: {stats.get('transfersCreated', '?'):,}")
            print(f"   Provider         : {stats.get('provider', '?')}")

        return stats

    @staticmethod
    def _escape_groovy_string(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def _build_loader_script(self, csv_path: str, max_rows: int) -> str:
        path = self._escape_groovy_string(csv_path)
        rows = int(max_rows)
        return f"""
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.tinkerpop.gremlin.structure.Vertex
import java.nio.file.Files
import java.nio.file.Path
import java.nio.charset.StandardCharsets
import java.util.Locale

def csvPath = '{path}'
def maxRows = {rows}

g.E().drop().iterate()
g.V().drop().iterate()

def bankMap = [:]
def accountMap = [:]
def countryMap = [:]
def txMap = [:]

def belongsToSeen = [] as Set
def locatedInSeen = [] as Set
def sentViaSeen = [] as Set

def COUNTRIES = [
    ['US','United States','LOW','Americas','false'],
    ['GB','United Kingdom','LOW','Europe','false'],
    ['DE','Germany','LOW','Europe','false'],
    ['CH','Switzerland','MEDIUM','Europe','false'],
    ['HK','Hong Kong','MEDIUM','Asia','false'],
    ['SG','Singapore','LOW','Asia','false'],
    ['AE','UAE','MEDIUM','Middle East','false'],
    ['NG','Nigeria','HIGH','Africa','false'],
    ['KY','Cayman Islands','HIGH','Americas','true'],
    ['PA','Panama','HIGH','Americas','true']
]

for (c in COUNTRIES) {{
    def cv = g.addV('Country')
        .property('countryCode', c[0])
        .property('countryName', c[1])
        .property('riskLevel', c[2])
        .property('region', c[3])
        .property('fatfBlacklist', c[4])
        .next()
    countryMap[c[0]] = cv
}}

def rowsLoaded = 0
def alertsCreated = 0

def fmt = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true).build()
def reader = Files.newBufferedReader(Path.of(csvPath), StandardCharsets.UTF_8)
def parser = fmt.parse(reader)

try {{
    for (rec in parser) {{
        if (rowsLoaded >= maxRows) break

        def fromBankId = rec.get('from_bank')
        def fromAcctId = rec.get('from_account')
        def toBankId = rec.get('to_bank')
        def toAcctId = rec.get('to_account')
        def amount = Double.parseDouble(rec.get('amount_paid'))
        def currency = rec.get('payment_currency')
        def rawFormat = (rec.isMapped('payment_format') && rec.isSet('payment_format')) ? rec.get('payment_format') : 'UNKNOWN'
        def format = (rawFormat == null || rawFormat.trim().isEmpty()) ? 'UNKNOWN' : rawFormat
        def ts = rec.get('timestamp')
        def laundering = rec.get('is_laundering')
        def txId = rec.get('transaction_id')
        def channel = format.toLowerCase(Locale.ROOT).contains('wire') ? 'WIRE' : 'DIGITAL'

        def fromCC = COUNTRIES[Math.floorMod(fromBankId.hashCode(), COUNTRIES.size())][0]
        def toCC = COUNTRIES[Math.floorMod(toBankId.hashCode(), COUNTRIES.size())][0]

        Vertex fromBank = bankMap[fromBankId]
        if (fromBank == null) {{
            fromBank = g.addV('Bank')
                .property('bankId', fromBankId)
                .property('bankName', 'Bank-' + fromBankId)
                .property('countryCode', fromCC)
                .property('swiftCode', 'SW' + fromBankId.toUpperCase())
                .property('tier', String.valueOf(Math.floorMod(fromBankId.hashCode(), 3) + 1))
                .next()
            bankMap[fromBankId] = fromBank
        }}

        Vertex toBank = bankMap[toBankId]
        if (toBank == null) {{
            toBank = g.addV('Bank')
                .property('bankId', toBankId)
                .property('bankName', 'Bank-' + toBankId)
                .property('countryCode', toCC)
                .property('swiftCode', 'SW' + toBankId.toUpperCase())
                .property('tier', String.valueOf(Math.floorMod(toBankId.hashCode(), 3) + 1))
                .next()
            bankMap[toBankId] = toBank
        }}

         def fromKey = fromBankId + ':' + fromAcctId
         def toKey = toBankId + ':' + toAcctId
         def suspicious = '1'.equals(laundering)

         // Create diverse risk scores (10-95 range) and blocked status based on hash.
         // Keep riskScore numeric so gt()/lt() predicates work in Gremlin execution.
         def fromRiskHash = Math.floorMod(fromAcctId.hashCode() * 7, 100)
         def fromBlocked = (fromRiskHash < 10) ? 'true' : 'false'
         def fromRiskScore = (fromRiskHash < 50) ? (10 + fromRiskHash) : (70 + (fromRiskHash - 50) * 2)

         def toRiskHash = Math.floorMod(toAcctId.hashCode() * 13, 100)
         def toBlocked = (toRiskHash < 10) ? 'true' : 'false'
         def toRiskScore = (toRiskHash < 50) ? (15 + toRiskHash) : (75 + (toRiskHash - 50) * 2)

         // ~30% of accounts have no openedDate (will be NULL in SQL)
         def fromHasOpenedDate = Math.floorMod(fromAcctId.hashCode() * 11, 100) >= 30
         def toHasOpenedDate = Math.floorMod(toAcctId.hashCode() * 17, 100) >= 30

         Vertex fromAcct = accountMap[fromKey]
         if (fromAcct == null) {{
             def fromBuilder = g.addV('Account')
                 .property('accountId', fromAcctId)
                 .property('bankId', fromBankId)
                 .property('accountType', Math.floorMod(fromAcctId.hashCode(), 2) == 0 ? 'CORPORATE' : 'PERSONAL')
                 .property('riskScore', fromRiskScore)
                 .property('isBlocked', fromBlocked)
             if (fromHasOpenedDate) {{
                 fromBuilder.property('openedDate', '2020-01-01')
             }}
             fromAcct = fromBuilder.next()
             accountMap[fromKey] = fromAcct
         }}

         Vertex toAcct = accountMap[toKey]
         if (toAcct == null) {{
             def toBuilder = g.addV('Account')
                 .property('accountId', toAcctId)
                 .property('bankId', toBankId)
                 .property('accountType', Math.floorMod(toAcctId.hashCode(), 2) == 0 ? 'CORPORATE' : 'PERSONAL')
                 .property('riskScore', toRiskScore)
                 .property('isBlocked', toBlocked)
             if (toHasOpenedDate) {{
                 toBuilder.property('openedDate', '2020-01-01')
             }}
             toAcct = toBuilder.next()
             accountMap[toKey] = toAcct
         }}

        Vertex txVertex = txMap[txId]
        if (txVertex == null) {{
            txVertex = g.addV('Transaction')
                .property('transactionId', txId)
                .property('amount', amount)
                .property('currency', currency)
                .property('paymentFormat', format)
                .property('eventTime', ts)
                .property('isLaundering', laundering)
                .property('channel', channel)
                .next()
            txMap[txId] = txVertex
        }}

        fromAcct.addEdge('TRANSFER', toAcct,
            'transactionId', txId,
            'amount', amount,
            'currency', currency,
            'paymentFormat', format,
            'eventTime', ts,
            'isLaundering', laundering)

        if (belongsToSeen.add(fromKey)) {{
            fromAcct.addEdge('BELONGS_TO', fromBank,
                'since', '2020-01-01',
                'isPrimary', 'true')
        }}
        if (belongsToSeen.add(toKey)) {{
            toAcct.addEdge('BELONGS_TO', toBank,
                'since', '2020-01-01',
                'isPrimary', 'true')
        }}

        Vertex fromCountryV = countryMap[fromCC]
        Vertex toCountryV = countryMap[toCC]
        if (fromCountryV != null && locatedInSeen.add(fromBankId)) {{
            fromBank.addEdge('LOCATED_IN', fromCountryV, 'isHeadquarters', 'true')
        }}
        if (toCountryV != null && locatedInSeen.add(toBankId)) {{
            toBank.addEdge('LOCATED_IN', toCountryV, 'isHeadquarters', 'true')
        }}

        fromAcct.addEdge('RECORDED_AS', txVertex,
            'recordedAt', ts,
            'source', 'CSV_LOAD')

        if (toCountryV != null && sentViaSeen.add(fromKey + '->' + toCC)) {{
            fromAcct.addEdge('SENT_VIA', toCountryV,
                'channelType', format,
                'routedAt', ts)
        }}

        // Flag only a subset of suspicious accounts so notebook demos can show
        // both suspicious-and-flagged and suspicious-but-unflagged patterns.
        def shouldFlag = suspicious && (Math.floorMod((fromAcctId + ':' + txId).hashCode(), 100) < 35)
        if (shouldFlag) {{
            def severity = amount > 50000 ? 'HIGH' : 'MEDIUM'
            Vertex alert = g.addV('Alert')
                .property('alertId', 'ALERT-' + txId)
                .property('alertType', 'SUSPICIOUS_TRANSFER')
                .property('severity', severity)
                .property('status', 'OPEN')
                .property('raisedAt', ts)
                .next()
            fromAcct.addEdge('FLAGGED_BY', alert,
                'flaggedAt', ts,
                'reason', 'Suspicious outbound transfer')
            alertsCreated += 1
        }}

        rowsLoaded += 1
    }}
}} finally {{
    parser.close()
    reader.close()
}}

return [
    rowsLoaded: rowsLoaded,
    accountsCreated: accountMap.size(),
    banksCreated: bankMap.size(),
    countriesCreated: countryMap.size(),
    transactionsCreated: txMap.size(),
    alertsCreated: alertsCreated,
    transfersCreated: rowsLoaded,
    sourcePath: csvPath,
    maxRows: maxRows,
    provider: 'tinkergraph'
]
"""

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

