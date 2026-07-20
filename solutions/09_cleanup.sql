-- =====================================================================
-- 09_cleanup.sql  (run at the end)
--
-- IMPORTANT: CoCo Desktop is connected to this trial AS VHOLuser (with the
-- PAT). Do NOT drop VHOLuser from that CoCo connection — you would be dropping
-- the user/token your own session is authenticated with, which fails or kills
-- the connection mid-script.
--
-- Two parts:
--   Part A (lab objects)  — safe to run from CoCo or anywhere with ACCOUNTADMIN.
--   Part B (identity)     — OPTIONAL. Run it from a Snowsight worksheet signed in
--                           as your trial's own admin user (the one you created at
--                           signup), NOT as VHOLuser. It removes the user and its
--                           network policy and will terminate CoCo's connection.
--                           For a throwaway trial you can just skip Part B and let
--                           the trial expire.
-- =====================================================================

-- ---- Part A: lab objects (safe from CoCo) ---------------------------
USE ROLE ACCOUNTADMIN;
DROP DATABASE  IF EXISTS STREAMING_HOL;
DROP WAREHOUSE IF EXISTS HOL_WH;

-- ---- Part B: identity (run from Snowsight as your signup admin, NOT VHOLuser) ----
-- DROP USER           IF EXISTS VHOLuser;
-- DROP NETWORK POLICY IF EXISTS vhol_np;
