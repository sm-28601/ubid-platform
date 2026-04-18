"""
Database schema and initialization for the UBID platform.
Uses SQLite for zero-dependency deployment.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ubid.db")


def get_connection():
    """Get a database connection with row_factory set."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    # ── Source records (unified staging from all departments) ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_records (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_system   TEXT NOT NULL,
            source_id       TEXT NOT NULL,
            raw_name        TEXT,
            normalized_name TEXT,
            raw_address     TEXT,
            normalized_address TEXT,
            pincode         TEXT,
            pan             TEXT,
            gstin           TEXT,
            owner_name      TEXT,
            registration_date TEXT,
            category        TEXT,
            raw_json        TEXT,
            ingested_at     TEXT DEFAULT (datetime('now')),
            UNIQUE(source_system, source_id)
        )
    """)

    # ── UBID master table ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ubid_master (
            ubid            TEXT PRIMARY KEY,
            anchor_type     TEXT,
            anchor_value    TEXT,
            canonical_name  TEXT,
            canonical_address TEXT,
            pincode         TEXT,
            activity_status TEXT DEFAULT 'Unknown',
            status_updated_at TEXT,
            status_evidence TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Linkage table: UBID ↔ source record ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ubid_linkages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ubid            TEXT NOT NULL,
            source_system   TEXT NOT NULL,
            source_id       TEXT NOT NULL,
            confidence_score REAL,
            match_evidence  TEXT,
            linked_by       TEXT DEFAULT 'system',
            linked_at       TEXT DEFAULT (datetime('now')),
            is_active       INTEGER DEFAULT 1,
            FOREIGN KEY (ubid) REFERENCES ubid_master(ubid)
        )
    """)

    # ── Match candidates (for reviewer workflow) ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS match_candidates (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            record_a_id     INTEGER NOT NULL,
            record_b_id     INTEGER NOT NULL,
            similarity_score REAL,
            match_evidence  TEXT,
            status          TEXT DEFAULT 'pending',
            reviewed_by     TEXT,
            reviewed_at     TEXT,
            reviewer_notes  TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (record_a_id) REFERENCES source_records(id),
            FOREIGN KEY (record_b_id) REFERENCES source_records(id)
        )
    """)

    # ── Activity events ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS activity_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_system   TEXT NOT NULL,
            source_event_id TEXT,
            event_type      TEXT NOT NULL,
            event_date      TEXT NOT NULL,
            event_details   TEXT,
            matched_ubid    TEXT,
            match_confidence REAL,
            raw_identifier  TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Audit log ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            action_type     TEXT NOT NULL,
            ubid            TEXT,
            details         TEXT,
            performed_by    TEXT DEFAULT 'system',
            performed_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Reviewer feedback (for model improvement loop) ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reviewer_feedback (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id            INTEGER,
            decision            TEXT NOT NULL,
            confidence_at_decision REAL,
            reviewer_notes      TEXT,
            decided_at          TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (match_id) REFERENCES match_candidates(id)
        )
    """)

    # ── Indexes for performance ──
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_pan ON source_records(pan)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_gstin ON source_records(gstin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_pincode ON source_records(pincode)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_system ON source_records(source_system)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_linkage_ubid ON ubid_linkages(ubid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_linkage_source ON ubid_linkages(source_system, source_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_ubid ON activity_events(matched_ubid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_date ON activity_events(event_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_status ON match_candidates(status)")

    conn.commit()
    conn.close()
    print(f"[DB] Database initialized at {DB_PATH}")


if __name__ == "__main__":
    init_db()
