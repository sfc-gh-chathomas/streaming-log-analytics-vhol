-- =====================================================================
-- 01_bronze.sql  (backup for the Part 1 prompt)
-- CoCo builds this from a prompt; this is the answer key. Creates the lab
-- database, schema, Gen2 warehouse, and the raw streaming landing table.
-- Run as VHOLuser (ACCOUNTADMIN). The default pipe BRONZE_LOGS-STREAMING is
-- auto-created on first use by the Snowpipe Streaming SDK.
-- =====================================================================
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS STREAMING_HOL;
CREATE SCHEMA   IF NOT EXISTS STREAMING_HOL.LOGS;

-- Gen2 standard warehouse (GENERATION='2' is the current, recommended syntax).
CREATE OR REPLACE WAREHOUSE HOL_WH
  WAREHOUSE_SIZE = 'XSMALL'
  GENERATION = '2'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Streaming VHOL warehouse (Gen2)';

USE DATABASE STREAMING_HOL;
USE SCHEMA   LOGS;
USE WAREHOUSE HOL_WH;

CREATE OR REPLACE TABLE BRONZE_LOGS (
  PAYLOAD   VARIANT,
  LANDED_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Freshness check the presenter runs after starting the producer:
-- SELECT PAYLOAD:service::string AS service,
--        PAYLOAD:level::string   AS level,
--        DATEDIFF('second', LANDED_TS, CURRENT_TIMESTAMP()) AS seconds_ago
-- FROM BRONZE_LOGS ORDER BY LANDED_TS DESC LIMIT 20;
