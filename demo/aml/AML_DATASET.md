# AML Dataset Reference

## Overview

The Anti-Money Laundering (AML) dataset used in this project is derived from the
**IBM Transactions for Anti Money Laundering (AML)** dataset, publicly available on
[Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml).

It models a **synthetic financial transaction network** where accounts, banks, and
countries are connected by labelled money-transfer edges. A subset of transactions are
synthetically labelled as money-laundering (`is_laundering = 1`) following known
typology patterns.

---

## Dataset Variants

The Kaggle dataset ships in six variants along two dimensions:

| Variant | Illicit ratio | Approximate transaction count |
|---------|--------------|-------------------------------|
| **HI-Small** | High illicit ratio | ~5 million rows |
| **HI-Medium** | High illicit ratio | ~20 million rows |
| **HI-Large** | High illicit ratio | ~50 million rows |
| **LI-Small** | Low illicit ratio | ~5 million rows |
| **LI-Medium** | Low illicit ratio | ~20 million rows |
| **LI-Large** | Low illicit ratio | ~50 million rows |

> The demo and notebook workloads in this project use **HI-Small** as the default
> source and normalize it into `demo/data/aml-demo.csv` (default: first 100,000 rows).

---

## Raw Source Files (Kaggle)

Each variant ships with three files:

| File | Description |
|------|-------------|
| `HI-Small_Trans.csv` | Transaction records — one row per transfer |
| `HI-Small_accounts.csv` | Account metadata |
| `HI-Small_Patterns.txt` | Annotated laundering pattern blocks used to label the data |

### Raw Transaction Schema (`HI-Small_Trans.csv`)

| Column index | Column name | Example value | Description |
|---|---|---|---|
| 0 | `Timestamp` | `2022/09/01 00:03` | Date and time of transaction |
| 1 | `From Bank` | `021174` | Numeric bank code of sender |
| 2 | `Account` (from) | `800737690` | Account identifier of sender |
| 3 | `To Bank` | `012` | Numeric bank code of receiver |
| 4 | `Account` (to) | `80011F990` | Account identifier of receiver |
| 5 | `Amount Received` | `2848.96` | Amount received by destination |
| 6 | `Receiving Currency` | `Euro` | Currency of received amount |
| 7 | `Amount Paid` | `2848.96` | Amount paid by source |
| 8 | `Payment Currency` | `Euro` | Currency of paid amount |
| 9 | `Payment Format` | `ACH` | Payment method |
| 10 | `Is Laundering` | `1` | Label: `1` = suspicious, `0` = clean |

---

## Normalized Loader Format (`aml-demo.csv`)

The `scripts/normalize_aml.py` script converts the raw Kaggle file into a simplified
format consumed by `AmlCsvLoader`:

| Column | Source column | Description |
|--------|--------------|-------------|
| `from_bank` | col 1 (`From Bank`) | Sender bank code |
| `from_account` | col 2 (`Account`) | Sender account ID |
| `to_bank` | col 3 (`To Bank`) | Receiver bank code |
| `to_account` | col 4 (`Account`) | Receiver account ID |
| `amount_paid` | col 7 (`Amount Paid`) | Transfer amount |
| `payment_currency` | col 8 (`Payment Currency`) | Currency of transfer |
| `payment_format` | col 9 (`Payment Format`) | Payment method (ACH, Wire, etc.) |
| `timestamp` | col 0 (`Timestamp`) | Transaction datetime |
| `is_laundering` | col 10 (`Is Laundering`) | `1` = suspicious, `0` = clean |
| `transaction_id` | synthetic (row index) | Sequential ID assigned during normalization |

---

## Currencies in the Dataset

The dataset contains transactions in **14 real-world currencies**:

`US Dollar` · `Euro` · `UK Pound` · `Yuan` · `Ruble` · `Rupee` · `Yen` ·
`Swiss Franc` · `Australian Dollar` · `Canadian Dollar` · `Mexican Peso` ·
`Brazilian Real` · `Saudi Riyal` · `Shekel`

Currency is **not converted** — amounts are recorded in the original payment currency.

---

## Payment Formats

| Format | Description |
|--------|-------------|
| `ACH` | Automated Clearing House (dominant in the dataset) |
| `Wire` | International wire transfer |
| `Cheque` | Paper cheque |
| `Credit Card` | Card payment |
| `Reinvestment` | Internal reinvestment transaction |
| `Cash` | Cash transaction |

---

## Laundering Pattern Typologies (`HI-Small_Patterns.txt`)

The patterns file annotates every synthetic laundering attempt with a named typology.
These correspond to known AML investigation patterns:

| Pattern | Description |
|---------|-------------|
| **FAN-OUT** | One account sends to many accounts (structuring / smurfing) |
| **FAN-IN** | Many accounts send to one account (aggregation / consolidation) |
| **CYCLE** | Circular chain of transfers returning funds to the origin account |
| **GATHER-SCATTER** | Multiple sources → single aggregator → multiple destinations |
| **SCATTER-GATHER** | Single source → multiple intermediaries → single destination |
| **BIPARTITE** | Two disjoint groups of accounts transacting only between groups |
| **STACK** | Sequential layering — A→B→C where each pair appears as a 2-hop chain |
| **RANDOM** | Random-hop chain (up to N hops) used as a noise baseline |

Each pattern block in the file specifies:
- A **type** and **degree** (e.g. `FAN-OUT: Max 16-degree Fan-Out`)
- The individual **transaction rows** that constitute the pattern
- A time window (typically a 7–10 day window starting `2022/09/01`)

---

## Graph Model

When loaded into the engine via `AmlCsvLoader`, the CSV is expanded into a **property graph**
with 5 vertex labels and 6 edge labels.

### Vertex Labels

| Label | SQL Table | Key Properties |
|-------|-----------|----------------|
| `Account` | `aml_accounts` | `accountId`, `bankId`, `accountType` (`PERSONAL`/`CORPORATE`), `riskScore` (10–95), `isBlocked`, `openedDate` |
| `Bank` | `aml_banks` | `bankId`, `bankName` (`Bank-<id>`), `countryCode`, `swiftCode`, `tier` (1–3) |
| `Transaction` | `aml_transactions` | `transactionId`, `amount`, `currency`, `paymentFormat`, `eventTime`, `isLaundering`, `channel` |
| `Country` | `aml_countries` | `countryCode`, `countryName`, `riskLevel` (`LOW`/`MEDIUM`/`HIGH`), `region`, `fatfBlacklist` |
| `Alert` | `aml_alerts` | `alertId`, `alertType` (`SUSPICIOUS_TRANSFER`), `severity` (`HIGH`/`MEDIUM`), `status` (`OPEN`), `raisedAt` |

### Edge Labels

| Label | SQL Table | Direction | Properties |
|-------|-----------|-----------|------------|
| `TRANSFER` | `aml_transfers` | `Account` → `Account` | `transactionId`, `amount`, `currency`, `paymentFormat`, `eventTime`, `isLaundering` |
| `BELONGS_TO` | `aml_account_bank` | `Account` → `Bank` | `since`, `isPrimary` |
| `LOCATED_IN` | `aml_bank_country` | `Bank` → `Country` | `isHeadquarters` |
| `RECORDED_AS` | `aml_transfer_transaction` | `Account` → `Transaction` | `recordedAt`, `source` |
| `FLAGGED_BY` | `aml_account_alert` | `Account` → `Alert` | `flaggedAt`, `reason` |
| `SENT_VIA` | `aml_transfer_channel` | `Account` → `Country` | `channelType`, `routedAt` |

### Graph Schema Diagram

```
                     LOCATED_IN
        Bank  ──────────────────►  Country
         ▲                              ▲
         │ BELONGS_TO                   │ SENT_VIA
         │                              │
      Account ──── TRANSFER ────►  Account
         │
         ├──── RECORDED_AS ──────►  Transaction
         │
         └──── FLAGGED_BY  ──────►  Alert
```

---

## Synthetic Enrichments Added by the Loader

`AmlCsvLoader` adds properties not present in the raw CSV to make the demo dataset
richer for graph queries:

| Enrichment | How it is derived |
|------------|------------------|
| `accountType` | Hash of `accountId` mod 2 → `CORPORATE` or `PERSONAL` |
| `riskScore` | Hash-derived integer in range **10–95** |
| `isBlocked` | `"true"` if derived risk hash < 10 (≈10% of accounts) |
| `openedDate` | Present for ~70% of accounts (`2020-01-01`); absent (NULL) for the rest |
| `bankName` | `"Bank-<bankId>"` |
| `swiftCode` | `"SW<BANKID>"` |
| `tier` | Hash of `bankId` mod 3 + 1 → `"1"`, `"2"`, or `"3"` |
| `countryCode` | Hash of `bankId` → one of 10 predefined country codes (see table below) |
| `channel` | `"WIRE"` if payment format contains "wire", else `"DIGITAL"` |
| Alerts | Created for ~35% of suspicious (`is_laundering=1`) transfers; `severity = HIGH` if `amount > 50,000`, else `MEDIUM` |

### Predefined Countries Seeded by the Loader

| Code | Country | Risk Level | Region | FATF Blacklist |
|------|---------|------------|--------|----------------|
| `US` | United States | LOW | Americas | No |
| `GB` | United Kingdom | LOW | Europe | No |
| `DE` | Germany | LOW | Europe | No |
| `CH` | Switzerland | MEDIUM | Europe | No |
| `HK` | Hong Kong | MEDIUM | Asia | No |
| `SG` | Singapore | LOW | Asia | No |
| `AE` | UAE | MEDIUM | Middle East | No |
| `NG` | Nigeria | HIGH | Africa | No |
| `KY` | Cayman Islands | HIGH | Americas | Yes |
| `PA` | Panama | HIGH | Americas | Yes |

---

## Obtaining the Dataset

```bash
# 1. Install the Kaggle CLI
pip install kaggle

# 2. Set up credentials
#    Go to https://www.kaggle.com/settings → API → Create New Token
#    Save the token to: ~/.kaggle/access_token   (or ~/.kaggle/kaggle.json)

# 3. Accept the dataset usage rules at:
#    https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml

# 4. Download and normalize (HI-Small variant, 100k rows)
./scripts/download_aml_data.sh --variant HI-Small --rows 100000

# Output files:
#   demo/data/HI-Small_Trans.csv        (raw)
#   demo/data/HI-Small_accounts.csv     (raw)
#   demo/data/HI-Small_Patterns.txt     (raw)
#   demo/data/aml-demo.csv              (normalized, ready for loader)
```

---

## Loading into the Engine

```python
from demo.aml_csv_loader import AmlCsvLoader

loader = AmlCsvLoader(base_url="http://localhost:7000")

# Upload the AML mapping
loader.upload_mapping("mappings/aml-mapping.json", mapping_id="aml")

# Load the CSV into TinkerGraph
stats = loader.load("demo/data/aml-demo.csv", max_rows=100_000)
print(stats)
# Expected output (approximate):
# {
#   'rowsLoaded':          100000,
#   'accountsCreated':     ~30000,
#   'banksCreated':        ~400,
#   'countriesCreated':    10,
#   'transactionsCreated': ~100000,
#   'alertsCreated':       ~1200,
#   'transfersCreated':    100000,
#   'provider':            'tinkergraph'
# }
```

---

## Mapping Files

| File | Backend | Used by |
|------|---------|---------|
| `mappings/aml-mapping.json` | H2 / Standard SQL | `aml_sql_showcase.ipynb` (`BACKEND='h2'`) |
| `mappings/iceberg-local-mapping.json` | Trino + Iceberg | `aml_sql_showcase.ipynb` (`BACKEND='iceberg'`) |

---

## Related Files

| Path | Description |
|------|-------------|
| `demo/data/HI-Small_Trans.csv` | Raw Kaggle transaction file |
| `demo/data/HI-Small_accounts.csv` | Raw Kaggle account metadata |
| `demo/data/HI-Small_Patterns.txt` | Labelled laundering pattern blocks |
| `demo/data/aml-demo.csv` | Normalized loader-ready CSV |
| `data/aml-demo.csv` | Copy used by standalone engine startup |
| `scripts/download_aml_data.sh` | Download + normalize automation script |
| `scripts/normalize_aml.py` | CSV column normalizer |
| `demo/aml_csv_loader.py` | Python loader (HTTP → Graph Query Engine) |
| `aml_demo_queries.ipynb` | AML Gremlin + complex query demo notebook |
| `aml_sql_showcase.ipynb` | Full SQL-capability showcase (H2 + Iceberg, flip `BACKEND`) |

---

## Attribution

> **IBM Transactions for Anti Money Laundering (AML)**  
> Author: E. Altman (IBM Research)  
> Source: [Kaggle — ealtman2019/ibm-transactions-for-anti-money-laundering-aml](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)  
> License: see Kaggle dataset page — usage rules must be accepted before download.

