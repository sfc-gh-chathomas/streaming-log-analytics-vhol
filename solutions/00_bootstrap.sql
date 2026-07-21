-- =====================================================================
-- 00_bootstrap.sql  (RUN ONCE in Snowsight as your signup ACCOUNTADMIN)
-- Identity + account settings ONLY. This creates the VHOLuser login and its
-- PAT, which CoCo Desktop and the producer authenticate with. CoCo can't
-- create its own login, so this must run first. Everything else — database,
-- schema, warehouse, tables — you build by prompting CoCo (see the README walkthrough).
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- Standardize on UTC so the producer's event times (UTC) and CURRENT_TIMESTAMP()
-- agree. Without this, per-layer freshness math is off by your local UTC offset.
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- Lab user for the CoCo connection and the producer. ACCOUNTADMIN so CoCo can
-- build the rest of the pipeline (database, warehouse, tables) via prompts.
CREATE USER IF NOT EXISTS VHOLuser
  DEFAULT_ROLE = ACCOUNTADMIN
  COMMENT = 'Streaming VHOL lab user';
GRANT ROLE ACCOUNTADMIN TO USER VHOLuser;

-- PATs require the user to be under a network policy, so open a permissive one.
CREATE NETWORK POLICY IF NOT EXISTS vhol_np ALLOWED_IP_LIST = ('0.0.0.0/0');
ALTER USER VHOLuser SET NETWORK_POLICY = vhol_np;

ALTER USER VHOLuser
  ADD PROGRAMMATIC ACCESS TOKEN vhol_pat
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = 7
    COMMENT = 'Streaming VHOL lab token';
-- >>> Copy the token_secret value now (shown once). <<<

-- The account identifier for CoCo. Paste this ONE value into CoCo Desktop's
-- "Account identifier" field (step 4). It is the org-account form (e.g. MYORG-MYACCT).
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
-- >>> Copy account_identifier. <<<

-- To revoke this token later without dropping the user:
--   ALTER USER VHOLuser REMOVE PROGRAMMATIC ACCESS TOKEN vhol_pat;
-- (List tokens: SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER VHOLuser;)
