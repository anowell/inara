-- Test schema teardown
-- Removes everything created by setup.sql.
-- Run with: just db-test-teardown

DROP SCHEMA IF EXISTS inara_test CASCADE;
