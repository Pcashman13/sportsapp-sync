"""
pg_compat.py
============
A thin SQLite-shaped wrapper around a psycopg2 connection.

The refresh_* scripts were written against sqlite3: they call
conn.execute(...) directly, use ? placeholders for positional parameters
and :named ones for dicts, and iterate the returned cursor. psycopg2 does
none of that — it has no connection-level execute(), and it wants %s and
%(named)s.

Rather than rewrite each script, they connect through here and keep their
existing SQL. The translation is deliberately small:

    ?          ->  %s           (when params is a tuple/list)
    :name      ->  %(name)s     (when params is a dict; :: casts are left)

Two things to know when writing new SQL for this:
  * A literal % in SQL must be written %% once parameters are passed.
  * ON CONFLICT ... DO UPDATE SET x = excluded.x is already valid Postgres,
    so upserts carry over unchanged.

Usage:
    from pg_compat import connect
    conn = connect()
    row = conn.execute("SELECT id FROM teams WHERE abbreviation=?", ("CHI",)).fetchone()
    conn.commit()
"""

import re
from db_connect import get_conn

# :name, but not the :: cast operator and not a lone colon.
_NAMED = re.compile(r'(?<![:\w]):([a-zA-Z_][a-zA-Z0-9_]*)')


def _translate(sql, params):
    if params is None:
        return sql
    if isinstance(params, dict):
        return _NAMED.sub(r'%(\1)s', sql)
    return sql.replace('?', '%s')


class PgConn:
    """Quacks like a sqlite3.Connection for the handful of methods used."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        # A fresh cursor each time, so a result set stays readable after the
        # next execute — sqlite3 code leans on that.
        cur = self._conn.cursor()
        cur.execute(_translate(sql, params), params)
        return cur

    def executemany(self, sql, seq):
        cur = self._conn.cursor()
        seq = list(seq)
        if seq:
            cur.executemany(_translate(sql, seq[0]), seq)
        return cur

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    @property
    def raw(self):
        """The underlying psycopg2 connection, for anything this doesn't cover."""
        return self._conn


def connect():
    return PgConn(get_conn())
