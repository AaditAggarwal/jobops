-- 003_sponsors.sql  (DESIGN.md §5.2; numbered 003 because 002 took jobs.note)
CREATE TABLE sponsor_records (
    id            BIGSERIAL PRIMARY KEY,
    src           TEXT NOT NULL,               -- uscis_hub | dol_lca
    fiscal_year   INT,
    employer_raw  TEXT NOT NULL,
    employer_norm TEXT NOT NULL,
    initial_approvals INT, initial_denials INT,
    continuing_approvals INT,
    soc_code TEXT, job_title TEXT, wage NUMERIC, worksite_state TEXT
);
CREATE INDEX sponsor_norm_trgm ON sponsor_records USING gin (employer_norm gin_trgm_ops);
CREATE INDEX sponsor_norm_idx  ON sponsor_records(employer_norm);
