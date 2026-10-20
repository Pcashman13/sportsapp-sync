"""
db_connect.py  (repo copy — no credentials)
===========================================
The connection string comes from the SUPABASE_DB_URL environment variable.
On GitHub Actions that is a repository secret; nothing is stored in this
file, which is why this copy is safe to commit.

The copy in ~/Desktop/SportsApp keeps a local fallback with the password
in it, and is never committed anywhere.
"""

import os
import psycopg2
import psycopg2.extras


def db_url():
    url = os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError(
            "SUPABASE_DB_URL is not set. On GitHub this comes from the "
            "repository secret of the same name."
        )
    return url


def get_conn():
    conn = psycopg2.connect(db_url())
    conn.autocommit = False
    return conn


def get_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
