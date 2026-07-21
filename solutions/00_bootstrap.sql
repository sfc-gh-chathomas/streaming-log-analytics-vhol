-- =====================================================================
-- 00_bootstrap.sql  (RUN ONCE in Snowsight as your signup ACCOUNTADMIN)
-- Identity + account settings ONLY. This creates the VHOLuser login and its
-- PAT, which CoCo Desktop and the producer authenticate with. CoCo can't
-- create its own login, so this must run first. Everything else — database,
-- schema, warehouse, tables — you build by prompting CoCo (see the README walkthrough).
--
-- HOW TO RUN: highlight one block and run it (Cmd/Ctrl+Enter).
--   BLOCK 1 — identity + PAT. Run it, then copy the PAT into your secret.pat.
--   BLOCK 2 — print your account identifier for the CoCo connection.
-- =====================================================================


-- ============================ BLOCK 1: identity + PAT ============================
-- Highlight from USE ROLE down to the ADD PROGRAMMATIC ACCESS TOKEN statement and run.
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
-- >>> Copy the token_secret value now (shown once) into your secret.pat file. <<<


-- ============================ BLOCK 2: account identifier ============================
-- Highlight and run this one line. Paste the value into CoCo Desktop's "Account
-- identifier" field (step 4). It is the org-account form (e.g. MYORG-MYACCT).
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
-- >>> Copy account_identifier. <<<


-- ============================ OPTIONAL: teardown (run later) ============================
-- Removes the lab identity and its access. Run these from Snowsight as your signup
-- admin, NOT from CoCo — CoCo is connected AS VHOLuser, so it cannot drop the user
-- it is authenticated with. Lab objects (database, warehouse) are dropped in 09_cleanup.sql.
-- ALTER USER VHOLuser REMOVE PROGRAMMATIC ACCESS TOKEN vhol_pat;  -- revoke just the token
-- DROP USER IF EXISTS VHOLuser;                                   -- remove the user and its access
-- DROP NETWORK POLICY IF EXISTS vhol_np;                          -- remove the network policy
-- (List tokens: SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER VHOLuser;)
