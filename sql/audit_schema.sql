-- =============================================================
-- ADDITIVE AUDIT SCHEMA (Data Refresh Policy)
-- Isolated from the analytical model: no existing table references
-- these tables, and no analytical query touches them.
--
-- Applied idempotently at runtime by etl_audit.py
-- (CREATE TABLE IF NOT EXISTS). This file is the documented copy.
-- Rollback: DROP TABLE etl_batch_log; DROP TABLE etl_watermark;
-- =============================================================

USE aviation_dw;

CREATE TABLE IF NOT EXISTS etl_batch_log (
    run_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_guid     CHAR(36)     NOT NULL,
    source_name    VARCHAR(30)  NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    started_at     DATETIME(3)  NOT NULL,
    finished_at    DATETIME(3)  NULL,
    rows_read      INT          NULL,
    rows_before    BIGINT       NULL,
    rows_after     BIGINT       NULL,
    rows_inserted  BIGINT       NULL,
    rows_updated   BIGINT       NULL,
    rows_skipped   BIGINT       NULL,
    notes          VARCHAR(500) NULL,
    error_detail   TEXT         NULL,
    engine_version VARCHAR(30)  NULL,
    CONSTRAINT chk_batch_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    CONSTRAINT chk_batch_source
        CHECK (source_name IN ('csv', 'weather', 'oltp', 'verify')),
    INDEX idx_batch_source_time (source_name, started_at),
    INDEX idx_batch_guid (batch_guid)
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    source_name    VARCHAR(30) NOT NULL,
    window_key     VARCHAR(50) NOT NULL,
    watermark_value VARCHAR(50) NOT NULL,
    updated_at     DATETIME(3) NOT NULL,
    last_batch_guid CHAR(36)   NULL,
    PRIMARY KEY (source_name, window_key)
);
