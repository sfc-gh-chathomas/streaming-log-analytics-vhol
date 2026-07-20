-- =====================================================================
-- 09_cleanup.sql  (run at the end)
-- =====================================================================
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS STREAMING_HOL;
DROP WAREHOUSE IF EXISTS HOL_WH;
DROP USER IF EXISTS VHOLuser;
DROP NETWORK POLICY IF EXISTS vhol_np;
