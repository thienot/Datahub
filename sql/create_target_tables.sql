CREATE TABLE IF NOT EXISTS qttg_header (
    id                  BIGSERIAL PRIMARY KEY,
    full_name           VARCHAR(50),
    nld_id              BIGINT NOT NULL UNIQUE,
    so_so_bhxh          VARCHAR(50) NOT NULL UNIQUE,
    thang_bd            VARCHAR(10),
    thang_kt            VARCHAR(10),
    created_at          TIMESTAMP NOT NULL DEFAULT now(),
    updated_at          TIMESTAMP NOT NULL DEFAULT now(),

    created_by          VARCHAR(100) NOT NULL DEFAULT 'spark-job',
    updated_by          VARCHAR(100) NOT NULL DEFAULT 'spark-job',
    batch_id            VARCHAR(50),                    -- mã lần chạy Spark job
    is_deleted          BOOLEAN NOT NULL DEFAULT FALSE  -- soft delete
);

CREATE TABLE IF NOT EXISTS qttg_detail (
    id              BIGSERIAL PRIMARY KEY,
    header_id       BIGINT NOT NULL REFERENCES qttg_header(id) ON DELETE CASCADE,
    ma_don_vi       VARCHAR(20),
    ten_don_vi      VARCHAR(500),
    tu_thang        VARCHAR(20),
    den_thang       VARCHAR(20),
    chuc_danh       VARCHAR(200),
    noi_lam_viec    VARCHAR(50),
    muc_luong       NUMERIC(19,4),
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now(),
    created_by      VARCHAR(100) NOT NULL DEFAULT 'spark-job',
    updated_by      VARCHAR(100) NOT NULL DEFAULT 'spark-job',
    batch_id        VARCHAR(50),
    is_deleted      BOOLEAN NOT NULL DEFAULT FALSE
);

-- Đánh index cho cả 2 điều kiện search: theo mã BHXH và theo mã người lao động
CREATE INDEX IF NOT EXISTS idx_qttg_header_so_bhxh ON qttg_header (so_so_bhxh);
CREATE INDEX IF NOT EXISTS idx_qttg_header_nld_id ON qttg_header (nld_id);

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_qttg_header_full_name_trgm ON qttg_header USING gin (full_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_qttg_detail_header_id ON qttg_detail (header_id);

