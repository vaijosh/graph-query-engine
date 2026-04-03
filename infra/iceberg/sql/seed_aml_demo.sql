CREATE SCHEMA IF NOT EXISTS iceberg.aml;

CREATE TABLE IF NOT EXISTS iceberg.aml.accounts (
    id BIGINT,
    account_id VARCHAR,
    bank_id VARCHAR,
    risk_score DOUBLE
);

CREATE TABLE IF NOT EXISTS iceberg.aml.banks (
    id BIGINT,
    bank_id VARCHAR,
    bank_name VARCHAR,
    country_code VARCHAR
);

ALTER TABLE iceberg.aml.banks ADD COLUMN IF NOT EXISTS country_code VARCHAR;

CREATE TABLE IF NOT EXISTS iceberg.aml.countries (
    id BIGINT,
    country_code VARCHAR,
    country_name VARCHAR,
    risk_level VARCHAR,
    region VARCHAR,
    fatf_blacklist VARCHAR
);

CREATE TABLE IF NOT EXISTS iceberg.aml.transfers (
    id BIGINT,
    out_id BIGINT,
    in_id BIGINT,
    amount DOUBLE,
    currency VARCHAR,
    is_laundering VARCHAR
);

ALTER TABLE iceberg.aml.transfers ADD COLUMN IF NOT EXISTS is_laundering VARCHAR;

CREATE TABLE IF NOT EXISTS iceberg.aml.bank_country (
    id BIGINT,
    out_id BIGINT,
    in_id BIGINT,
    is_headquarters VARCHAR
);

CREATE TABLE IF NOT EXISTS iceberg.aml.account_bank (
    id BIGINT,
    out_id BIGINT,
    in_id BIGINT,
    is_primary VARCHAR
);

CREATE TABLE IF NOT EXISTS iceberg.aml.alerts (
    id BIGINT,
    alert_id VARCHAR,
    alert_type VARCHAR,
    severity VARCHAR,
    status VARCHAR,
    raised_at VARCHAR
);

CREATE TABLE IF NOT EXISTS iceberg.aml.account_country (
    id BIGINT,
    out_id BIGINT,
    in_id BIGINT,
    channel_type VARCHAR,
    routed_at VARCHAR
);

CREATE TABLE IF NOT EXISTS iceberg.aml.account_alert (
    id BIGINT,
    out_id BIGINT,
    in_id BIGINT,
    flagged_at VARCHAR,
    reason VARCHAR
);

DELETE FROM iceberg.aml.accounts;
DELETE FROM iceberg.aml.banks;
DELETE FROM iceberg.aml.countries;
DELETE FROM iceberg.aml.transfers;
DELETE FROM iceberg.aml.bank_country;
DELETE FROM iceberg.aml.account_bank;
DELETE FROM iceberg.aml.alerts;
DELETE FROM iceberg.aml.account_country;
DELETE FROM iceberg.aml.account_alert;

INSERT INTO iceberg.aml.accounts VALUES
  (1, 'ACC-001', 'BANK-01', 0.91),
  (2, 'ACC-002', 'BANK-02', 0.41),
  (3, 'ACC-003', 'BANK-01', 0.75);

INSERT INTO iceberg.aml.banks (id, bank_id, bank_name, country_code) VALUES
  (101, 'BANK-01', 'Alpha Bank', 'SG'),
  (102, 'BANK-02', 'Beta Bank', 'KY');

INSERT INTO iceberg.aml.countries VALUES
  (201, 'SG', 'Singapore', 'LOW', 'Asia', 'false'),
  (202, 'KY', 'Cayman Islands', 'HIGH', 'Americas', 'true');

INSERT INTO iceberg.aml.transfers (id, out_id, in_id, amount, currency, is_laundering) VALUES
  (1001, 1, 2, 120000.00, 'USD', '1'),
  (1002, 3, 1, 45000.00, 'EUR', '0');

INSERT INTO iceberg.aml.bank_country VALUES
  (3001, 101, 201, 'true'),
  (3002, 102, 202, 'true');

INSERT INTO iceberg.aml.account_bank VALUES
  (4001, 1, 101, 'true'),
  (4002, 2, 102, 'true'),
  (4003, 3, 101, 'true');

INSERT INTO iceberg.aml.alerts VALUES
  (5001, 'ALERT-1001', 'SUSPICIOUS_TRANSFER', 'HIGH', 'OPEN', '2024-01-01T00:00:00Z'),
  (5002, 'ALERT-1002', 'SUSPICIOUS_TRANSFER', 'MEDIUM', 'OPEN', '2024-01-02T00:00:00Z');

INSERT INTO iceberg.aml.account_country VALUES
  (6001, 1, 202, 'WIRE', '2024-01-01T00:00:00Z'),
  (6002, 3, 201, 'DIGITAL', '2024-01-02T00:00:00Z');

INSERT INTO iceberg.aml.account_alert VALUES
  (7001, 1, 5001, '2024-01-01T00:00:00Z', 'Suspicious outbound transfer'),
  (7002, 3, 5002, '2024-01-02T00:00:00Z', 'Pattern escalation');

