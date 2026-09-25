"""证据一致性证据采信服务的 SQLite 模式与事务辅助。"""

from __future__ import annotations

import contextlib
import hashlib
import sqlite3
from collections.abc import Iterator
from pathlib import Path


SCHEMA_VERSION = 3

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evidence_protocol_catalog (
    evidence_protocol_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    title TEXT NOT NULL,
    task_family TEXT NOT NULL,
    canonical_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    created_at TEXT NOT NULL,
    PRIMARY KEY (evidence_protocol_id, version),
    UNIQUE (content_sha256)
);

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('operator', 'statistician', 'approver', 'auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS capture_devices (
    device_id TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    vendor TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS builds (
    build_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES capture_devices(device_id),
    version TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    created_at TEXT NOT NULL,
    UNIQUE (device_id, version),
    UNIQUE (content_sha256)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    evidence_protocol_id TEXT NOT NULL,
    evidence_protocol_version INTEGER NOT NULL,
    build_id TEXT NOT NULL REFERENCES builds(build_id),
    state TEXT NOT NULL CHECK (state IN ('draft', 'running', 'sealed', 'analyzing', 'analyzed', 'decided')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_by TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    started_at TEXT,
    sealed_at TEXT,
    FOREIGN KEY (evidence_protocol_id, evidence_protocol_version) REFERENCES evidence_protocol_catalog(evidence_protocol_id, version)
);

CREATE TABLE IF NOT EXISTS evidence_items (
    evidence_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    source_batch TEXT NOT NULL,
    source_row TEXT NOT NULL,
    device_id TEXT NOT NULL REFERENCES capture_devices(device_id),
    evidence_group_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    indicators_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    imported_by TEXT NOT NULL REFERENCES users(user_id),
    imported_at TEXT NOT NULL,
    UNIQUE (batch_id, source_batch, source_row)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (scope, key)
);

CREATE TABLE IF NOT EXISTS exclusion_requests (
    exclusion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_item_id INTEGER NOT NULL REFERENCES evidence_items(evidence_item_id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'revoked')),
    reason TEXT NOT NULL,
    requested_by TEXT NOT NULL REFERENCES users(user_id),
    requested_at TEXT NOT NULL,
    reviewed_by TEXT REFERENCES users(user_id),
    reviewed_at TEXT,
    review_note TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS one_open_exclusion_per_evidence_item
ON exclusion_requests(evidence_item_id)
WHERE status IN ('pending', 'approved');

-- 一份排除申请至多存在一个有效复核决定：主键由数据库强制，
-- 并发裁决中只有一个事务能插入成功，跨连接与进程重启均成立。
CREATE TABLE IF NOT EXISTS review_decisions (
    exclusion_id INTEGER PRIMARY KEY REFERENCES exclusion_requests(exclusion_id),
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected')),
    note TEXT NOT NULL,
    decided_by TEXT NOT NULL REFERENCES users(user_id),
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    idempotency_key TEXT,
    decided_at TEXT NOT NULL
);

-- 每次复核请求的裁决台账：有效决定 / 网络重试重放 / 竞争失败各占一行，
-- 竞争失败不写入 audit_events，因此这里是区分三类请求的唯一权威来源。
CREATE TABLE IF NOT EXISTS review_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    exclusion_id INTEGER NOT NULL REFERENCES exclusion_requests(exclusion_id),
    outcome TEXT NOT NULL CHECK (outcome IN ('effective', 'replay', 'lost_conflict')),
    requested_decision TEXT NOT NULL CHECK (requested_decision IN ('approved', 'rejected')),
    actor_id TEXT NOT NULL REFERENCES users(user_id),
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    idempotency_key TEXT,
    effective_exclusion_id INTEGER REFERENCES review_decisions(exclusion_id),
    attempted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS review_attempts_exclusion_idx
ON review_attempts(exclusion_id, attempt_id);

CREATE TABLE IF NOT EXISTS analysis_jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    batch_revision INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'leased', 'succeeded', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (batch_id, batch_revision)
);

CREATE TABLE IF NOT EXISTS analyses (
    analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    batch_revision INTEGER NOT NULL,
    evidence_protocol_sha256 TEXT NOT NULL CHECK (length(evidence_protocol_sha256) = 64),
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    algorithm_version TEXT NOT NULL,
    seed INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (batch_id, batch_revision, input_sha256)
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    analysis_id INTEGER NOT NULL REFERENCES analyses(analysis_id),
    decision TEXT NOT NULL CHECK (decision IN ('needs_more_data', 'approved', 'rejected')),
    reason TEXT NOT NULL,
    decided_by TEXT NOT NULL REFERENCES users(user_id),
    decided_at TEXT NOT NULL,
    UNIQUE (batch_id, analysis_id)
);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

REQUIRED_TABLES = frozenset({
    "schema_meta", "evidence_protocol_catalog", "users", "capture_devices", "builds", "batches",
    "evidence_items", "idempotency_keys", "exclusion_requests", "review_decisions",
    "review_attempts", "analysis_jobs", "analyses", "decisions", "audit_events",
})


def connect(path: str | Path) -> sqlite3.Connection:
    """打开连接并启用严格的事务与外键设置。"""

    connection = sqlite3.connect(str(path), isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


@contextlib.contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    """显式事务；异常时保证回滚。"""

    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def _backfill_review_decisions(connection: sqlite3.Connection) -> None:
    """把旧版本中已裁决但未登记的有效决定补入裁决表与台账。"""

    rows = connection.execute(
        "SELECT exclusion_id,status,review_note,reviewed_by,reviewed_at,requested_by,requested_at "
        "FROM exclusion_requests WHERE status IN ('approved','rejected') "
        "AND NOT EXISTS (SELECT 1 FROM review_decisions d WHERE d.exclusion_id=exclusion_requests.exclusion_id)"
    ).fetchall()
    for row in rows:
        basis = f"{row['exclusion_id']}|{row['status']}|{row['reviewed_by'] or ''}|{row['reviewed_at'] or ''}"
        digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()
        connection.execute(
            "INSERT INTO review_decisions(exclusion_id,decision,note,decided_by,request_sha256,"
            "idempotency_key,decided_at) VALUES(?,?,?,?,?,?,?)",
            (
                row["exclusion_id"], row["status"], row["review_note"] or "",
                row["reviewed_by"] or row["requested_by"], digest, None,
                row["reviewed_at"] or row["requested_at"],
            ),
        )
    connection.execute(
        "INSERT INTO review_attempts(exclusion_id,outcome,requested_decision,actor_id,request_sha256,"
        "idempotency_key,effective_exclusion_id,attempted_at) "
        "SELECT exclusion_id,'effective',decision,decided_by,request_sha256,NULL,exclusion_id,decided_at "
        "FROM review_decisions WHERE NOT EXISTS ("
        "SELECT 1 FROM review_attempts a WHERE a.exclusion_id=review_decisions.exclusion_id)"
    )


def initialize(connection: sqlite3.Connection) -> None:
    """初始化基础资料表，重复执行不改变已有数据。"""

    connection.executescript(SCHEMA_SQL)
    with transaction(connection, immediate=True):
        _backfill_review_decisions(connection)
        connection.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )


def inspect_schema(connection: sqlite3.Connection) -> dict[str, object]:
    """返回适合机器检查的数据库结构摘要。"""

    table_rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    tables = tuple(row["name"] for row in table_rows)
    version_row = connection.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()
    missing = sorted(REQUIRED_TABLES - set(tables))
    foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    return {
        "tables": tables,
        "missing_tables": missing,
        "schema_version": None if version_row is None else version_row["value"],
        "foreign_keys": bool(foreign_keys),
    }
