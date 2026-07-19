-- 002_jobs_note.sql
-- Free-text note on jobs, used by enrich/dedup to explain why a row was
-- skipped (e.g. "duplicate of <id> via greenhouse").
ALTER TABLE jobs ADD COLUMN note TEXT;
