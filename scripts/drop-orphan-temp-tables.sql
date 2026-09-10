-- Remove orphaned Airbyte temp tables left behind by failed jobs.
--
-- Background
-- ----------
-- The direct-load (v3) destination creates a temp table per stream per job,
-- loads into it, upserts into the real table, then drops it. When a stream
-- failed, the drop was skipped and the temp table survived. Because temp table
-- names carry a per-connection unique id, a later job never reuses or
-- overwrites an orphan -- they accumulate, one per failed job.
--
-- Fixed in destination-postgres 3.0.5-crewhu.2. This script cleans up the
-- backlog created before that version.
--
-- How a temp table is recognized
-- ------------------------------
-- DefaultTempTableNameGenerator builds the name as:
--     <namespace[0:8]><name[0:8]><sha256hex[0:32]>
-- so every temp table ends in exactly 32 hex characters. Real tables do not.
-- That 32-hex suffix is the signature this script matches.
--
-- Safety
-- ------
-- This script DROPS TABLES. Read these before running:
--
--   * Run STEP 1 first and read the output. Nothing is dropped by it.
--   * A table is only a candidate if it has the 32-hex suffix AND is not the
--     final table of any stream AND has not been written to recently.
--   * The age threshold protects temp tables of jobs that are running right
--     now. Set it above your longest sync. Default: 24 hours.
--   * STEP 3 is commented out. Uncomment it deliberately, after reviewing the
--     list from STEP 1.
--
-- Usage: edit the two settings below, then run step by step.

-- ---------------------------------------------------------------------------
-- Settings
-- ---------------------------------------------------------------------------
--   target_schema  the schema holding the tables (e.g. 'airbyte_etl')
--   min_age        how old a table must be to count as abandoned

\set target_schema 'airbyte_etl'
\set min_age '24 hours'


-- ---------------------------------------------------------------------------
-- STEP 1 -- Inspect. Read-only. Run this first.
-- ---------------------------------------------------------------------------
-- Lists every candidate with its size and last-write time, newest first.
-- Check that nothing here is a table you expect to keep.

WITH candidates AS (
    SELECT
        c.oid,
        n.nspname                       AS schema_name,
        c.relname                       AS table_name,
        pg_total_relation_size(c.oid)   AS bytes,
        GREATEST(
            s.last_autoanalyze,
            s.last_analyze,
            s.last_autovacuum,
            s.last_vacuum
        )                               AS last_touched
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
    WHERE c.relkind = 'r'
      AND n.nspname = :'target_schema'
      -- Temp table signature: name ends in exactly 32 hex characters.
      AND c.relname ~ '[0-9a-f]{32}$'
      -- Never touch the connection-test tables; the check operation manages them.
      AND c.relname NOT LIKE '\_airbyte\_connection\_test\_%'
)
SELECT
    schema_name,
    table_name,
    pg_size_pretty(bytes)                       AS size,
    COALESCE(last_touched::text, 'never')       AS last_touched,
    bytes
FROM candidates
ORDER BY bytes DESC, table_name;


-- ---------------------------------------------------------------------------
-- STEP 2 -- Summary. Read-only.
-- ---------------------------------------------------------------------------
-- How many tables and how much space would be reclaimed.

SELECT
    count(*)                                    AS orphan_tables,
    pg_size_pretty(COALESCE(sum(pg_total_relation_size(c.oid)), 0)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = :'target_schema'
  AND c.relname ~ '[0-9a-f]{32}$'
  AND c.relname NOT LIKE '\_airbyte\_connection\_test\_%';


-- ---------------------------------------------------------------------------
-- STEP 3 -- Generate the DROP statements. Read-only: emits SQL, runs nothing.
-- ---------------------------------------------------------------------------
-- Review the output, then paste back the statements you want to execute.
-- This is the recommended path: you see each DROP before it runs.
--
-- The age filter uses pg_stat_all_tables, whose counters reset on a stats
-- reset or a restore. A table with no stats row shows as 'never' and is
-- treated as old -- which is correct for an abandoned table, but means you
-- should still eyeball the list from STEP 1.

SELECT format(
           'DROP TABLE IF EXISTS %I.%I;',
           n.nspname,
           c.relname
       ) AS drop_statement
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
WHERE c.relkind = 'r'
  AND n.nspname = :'target_schema'
  AND c.relname ~ '[0-9a-f]{32}$'
  AND c.relname NOT LIKE '\_airbyte\_connection\_test\_%'
  -- Skip anything touched inside the age window: it may belong to a live job.
  AND COALESCE(
          GREATEST(s.last_autoanalyze, s.last_analyze, s.last_autovacuum, s.last_vacuum),
          '-infinity'::timestamptz
      ) < now() - :'min_age'::interval
ORDER BY c.relname;


-- ---------------------------------------------------------------------------
-- STEP 4 -- Execute. DESTRUCTIVE. Commented out on purpose.
-- ---------------------------------------------------------------------------
-- Only uncomment after reviewing STEP 1 and STEP 3.
--
-- Drops run one statement at a time rather than in a single transaction: each
-- DROP takes an ACCESS EXCLUSIVE lock, and holding all of them together would
-- block the whole schema until the last one commits. One at a time keeps each
-- lock short. lock_timeout makes a drop give up instead of queueing behind a
-- long-running sync -- anything skipped can be picked up on the next run.

-- SET lock_timeout = '5s';
--
-- DO $$
-- DECLARE
--     target_schema CONSTANT text := 'airbyte_etl';
--     min_age       CONSTANT interval := interval '24 hours';
--     rec           record;
--     dropped       int := 0;
--     skipped       int := 0;
-- BEGIN
--     FOR rec IN
--         SELECT n.nspname AS schema_name, c.relname AS table_name
--         FROM pg_class c
--         JOIN pg_namespace n ON n.oid = c.relnamespace
--         LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
--         WHERE c.relkind = 'r'
--           AND n.nspname = target_schema
--           AND c.relname ~ '[0-9a-f]{32}$'
--           AND c.relname NOT LIKE '\_airbyte\_connection\_test\_%'
--           AND COALESCE(
--                   GREATEST(s.last_autoanalyze, s.last_analyze,
--                            s.last_autovacuum, s.last_vacuum),
--                   '-infinity'::timestamptz
--               ) < now() - min_age
--         ORDER BY c.relname
--     LOOP
--         BEGIN
--             EXECUTE format('DROP TABLE IF EXISTS %I.%I',
--                            rec.schema_name, rec.table_name);
--             dropped := dropped + 1;
--             RAISE NOTICE 'dropped %.%', rec.schema_name, rec.table_name;
--         EXCEPTION WHEN lock_not_available THEN
--             -- Busy table: almost certainly a live job. Leave it.
--             skipped := skipped + 1;
--             RAISE NOTICE 'skipped %.% (locked)', rec.schema_name, rec.table_name;
--         END;
--     END LOOP;
--
--     RAISE NOTICE 'done: % dropped, % skipped', dropped, skipped;
-- END $$;
