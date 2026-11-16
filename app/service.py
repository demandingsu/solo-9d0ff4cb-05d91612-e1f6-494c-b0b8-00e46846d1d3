"""Ingest, revocation, event reconciliation and cursor-based change feed.

Consistency contract
--------------------
* Every upload *and every revocation* is applied in exactly one SQLite
  ``BEGIN IMMEDIATE`` transaction.  Source records, raw points, current events
  and change-log entries therefore become visible together; a failure rolls all
  of them back and the change cursor does not advance.
* ``BEGIN IMMEDIATE`` serialises writers across processes/instances sharing the
  database file, so interleaved uploads/revocations cannot lose updates.
* ``(port_id, upload_key)`` is unique and is checked inside the write
  transaction.  A replayed upload returns the original result and emits no new
  changes.  The same ``upload_key`` with different content is rejected, and a
  replayed upload of a revoked batch reports the revocation instead of reviving
  it.
* Each observation records which batches sourced it (``point_sources``).
  Revoking a batch removes only points with no other live source; a shared
  point survives while any non-revoked batch still asserts it.
* Batches carry ``provenance_version``.  Only batches written with per-point
  provenance (current schema) may be revoked; older batches are refused because
  removing points without provenance could delete observations retransmitted by
  another batch.
* The change feed uses a monotonically increasing ``seq`` primary key as the
  cursor, so clients can page strictly forward and resume after restarts.
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

from . import events as event_rules
from .csv_ingest import RawPointRow
from .db import write_transaction
from .errors import AppError

#: Batches written by this code store one ``point_sources`` row per
#: observation they asserted.  Anything older has no complete provenance and
#: is not revocable.
PROVENANCE_VERSION = 1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Port:
    id: int
    code: str
    name: str
    min_lat_e6: int
    max_lat_e6: int
    min_lon_e6: int
    max_lon_e6: int

    def contains(self, lat_e6: int, lon_e6: int) -> bool:
        return (
            self.min_lat_e6 <= lat_e6 <= self.max_lat_e6
            and self.min_lon_e6 <= lon_e6 <= self.max_lon_e6
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "code": self.code,
            "name": self.name,
            "bounds": {
                "min_lat": self.min_lat_e6 / 1e6,
                "max_lat": self.max_lat_e6 / 1e6,
                "min_lon": self.min_lon_e6 / 1e6,
                "max_lon": self.max_lon_e6 / 1e6,
            },
        }


def _port_from_row(row: sqlite3.Row) -> Port:
    return Port(
        id=row["id"],
        code=row["code"],
        name=row["name"],
        min_lat_e6=row["min_lat_e6"],
        max_lat_e6=row["max_lat_e6"],
        min_lon_e6=row["min_lon_e6"],
        max_lon_e6=row["max_lon_e6"],
    )


def get_port(conn: sqlite3.Connection, code: str) -> Port:
    row = conn.execute(
        "SELECT * FROM ports WHERE code = ?", (code,)
    ).fetchone()
    if row is None:
        raise AppError(404, "port_not_found", f"port {code!r} is not registered")
    return _port_from_row(row)


@contextmanager
def _transaction_if_needed(
    conn: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    """Join an existing write transaction, or start one.

    Lets ``register_port``/``ingest`` be called standalone (own transaction) or
    from the demo handler, where both must commit or roll back together.
    """
    if conn.in_transaction:
        yield conn
        return
    with write_transaction(conn):
        yield conn


def register_port(
    conn: sqlite3.Connection,
    *,
    code: str,
    name: str,
    min_lat_e6: int,
    max_lat_e6: int,
    min_lon_e6: int,
    max_lon_e6: int,
) -> tuple[Port, bool]:
    """Idempotently register a rectangular port.  Returns (port, created)."""
    with _transaction_if_needed(conn):
        existing = conn.execute(
            "SELECT * FROM ports WHERE code = ?", (code,)
        ).fetchone()
        if existing is not None:
            same = (
                existing["name"] == name
                and existing["min_lat_e6"] == min_lat_e6
                and existing["max_lat_e6"] == max_lat_e6
                and existing["min_lon_e6"] == min_lon_e6
                and existing["max_lon_e6"] == max_lon_e6
            )
            if not same:
                raise AppError(
                    409,
                    "port_conflict",
                    f"port {code!r} already exists with a different definition",
                )
            return _port_from_row(existing), False
        cursor = conn.execute(
            """
            INSERT INTO ports
                (code, name, min_lat_e6, max_lat_e6, min_lon_e6, max_lon_e6,
                 created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (code, name, min_lat_e6, max_lat_e6, min_lon_e6, max_lon_e6,
             utc_now_iso()),
        )
        row = conn.execute(
            "SELECT * FROM ports WHERE id = ?", (cursor.lastrowid,)
        ).fetchone()
        return _port_from_row(row), True


def _batch_response(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM ingest_batches WHERE id = ?", (batch_id,)
    ).fetchone()
    # Only the upload's own effects; changes emitted by a later revocation use
    # operation='revoke' and are reported through the revocation response.
    change_rows = conn.execute(
        "SELECT change_type, event_id FROM event_changes "
        "WHERE batch_id = ? AND operation = 'ingest' ORDER BY seq",
        (batch_id,),
    ).fetchall()
    first = conn.execute(
        "SELECT MIN(seq) AS s FROM event_changes "
        "WHERE batch_id = ? AND operation = 'ingest'",
        (batch_id,),
    ).fetchone()["s"]
    last = conn.execute(
        "SELECT MAX(seq) AS s FROM event_changes "
        "WHERE batch_id = ? AND operation = 'ingest'",
        (batch_id,),
    ).fetchone()["s"]
    return {
        "batch_id": row["id"],
        "port_id": row["port_id"],
        "upload_key": row["upload_key"],
        "content_sha256": row["content_sha256"],
        "received_at": row["received_at"],
        "rows_received": row["rows_received"],
        "rows_new": row["rows_new"],
        "rows_duplicate": row["rows_duplicate"],
        "rows_outside_port": row["rows_outside_port"],
        "revocable": row["provenance_version"] >= PROVENANCE_VERSION,
        "revoked": row["revoked_at"] is not None,
        "revoked_at": row["revoked_at"],
        "event_changes": {
            "upserted": sum(1 for c in change_rows if c["change_type"] == "upsert"),
            "retracted": sum(
                1 for c in change_rows if c["change_type"] == "retract"
            ),
        },
        "change_cursor": {"from": first, "to": last},
    }


def _load_points_for_vessels(
    conn: sqlite3.Connection, port_id: int, vessel_ids: Iterable[str]
) -> dict[str, list[event_rules.Point]]:
    result: dict[str, list[event_rules.Point]] = {}
    for vessel_id in vessel_ids:
        rows = conn.execute(
            """
            SELECT id, vessel_id, event_time, lat_e6, lon_e6
            FROM raw_points
            WHERE port_id = ? AND vessel_id = ?
            ORDER BY event_time, id
            """,
            (port_id, vessel_id),
        ).fetchall()
        result[vessel_id] = [
            event_rules.Point(
                id=r["id"],
                vessel_id=r["vessel_id"],
                event_time=r["event_time"],
                lat_e6=r["lat_e6"],
                lon_e6=r["lon_e6"],
            )
            for r in rows
        ]
    return result


def _existing_events(
    conn: sqlite3.Connection, port_id: int, vessel_ids: Iterable[str]
) -> dict[str, list[sqlite3.Row]]:
    result: dict[str, list[sqlite3.Row]] = {}
    for vessel_id in vessel_ids:
        result[vessel_id] = conn.execute(
            """
            SELECT * FROM events
            WHERE port_id = ? AND vessel_id = ?
            ORDER BY start_time, id
            """,
            (port_id, vessel_id),
        ).fetchall()
    return result


def _event_matches_row(event: sqlite3.Row, stay: event_rules.Stay) -> bool:
    return (
        event["end_time"] == stay.end_time
        and event["start_lat_e6"] == stay.start_lat_e6
        and event["start_lon_e6"] == stay.start_lon_e6
        and event["end_lat_e6"] == stay.end_lat_e6
        and event["end_lon_e6"] == stay.end_lon_e6
        and event["point_count"] == stay.point_count
    )


def _append_change(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    port_id: int,
    change_type: str,
    event_id: int,
    stay: event_rules.Stay,
    created_at: str,
    operation: str = "ingest",
) -> None:
    conn.execute(
        """
        INSERT INTO event_changes
            (batch_id, operation, port_id, change_type, event_id, vessel_id,
             start_time, end_time, start_lat_e6, start_lon_e6, end_lat_e6,
             end_lon_e6, point_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            batch_id,
            operation,
            port_id,
            change_type,
            event_id,
            stay.vessel_id,
            stay.start_time,
            stay.end_time,
            stay.start_lat_e6,
            stay.start_lon_e6,
            stay.end_lat_e6,
            stay.end_lon_e6,
            stay.point_count,
            created_at,
        ),
    )


def _retract_row(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    port_id: int,
    event: sqlite3.Row,
    created_at: str,
    operation: str = "ingest",
) -> None:
    stay = event_rules.Stay(
        vessel_id=event["vessel_id"],
        start_time=event["start_time"],
        end_time=event["end_time"],
        start_lat_e6=event["start_lat_e6"],
        start_lon_e6=event["start_lon_e6"],
        end_lat_e6=event["end_lat_e6"],
        end_lon_e6=event["end_lon_e6"],
        point_count=event["point_count"],
    )
    _append_change(
        conn,
        batch_id=batch_id,
        port_id=port_id,
        change_type="retract",
        event_id=event["id"],
        stay=stay,
        created_at=created_at,
        operation=operation,
    )
    conn.execute("DELETE FROM events WHERE id = ?", (event["id"],))


def _reconcile_vessel(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    port: Port,
    vessel_id: str,
    points: list[event_rules.Point],
    existing: list[sqlite3.Row],
    created_at: str,
    operation: str = "ingest",
) -> None:
    stays = event_rules.derive_stays(points, port.contains)

    if len({stay.key for stay in stays}) != len(stays):
        raise RuntimeError("event derivation produced duplicate keys")
    old_by_key = {(row["vessel_id"], row["start_time"]): row for row in existing}
    new_by_key = {stay.key: stay for stay in stays}

    # 1. Retract events that no longer exist, or whose payload changed.
    kept_keys: set[tuple[str, int]] = set()
    for key, row in old_by_key.items():
        stay = new_by_key.get(key)
        if stay is not None and _event_matches_row(row, stay):
            kept_keys.add(key)
            continue
        _retract_row(
            conn,
            batch_id=batch_id,
            port_id=port.id,
            event=row,
            created_at=created_at,
            operation=operation,
        )

    # 2. Insert genuinely new events.  An event whose start moved arrived here
    #    because its old key was retracted above; a same-key payload change is
    #    represented as retract + insert so consumers see a full snapshot.
    for stay in stays:
        if stay.key in kept_keys:
            continue
        cursor = conn.execute(
            """
            INSERT INTO events
                (port_id, vessel_id, start_time, end_time, start_lat_e6,
                 start_lon_e6, end_lat_e6, end_lon_e6, point_count,
                 created_batch_id, updated_batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                port.id,
                stay.vessel_id,
                stay.start_time,
                stay.end_time,
                stay.start_lat_e6,
                stay.start_lon_e6,
                stay.end_lat_e6,
                stay.end_lon_e6,
                stay.point_count,
                batch_id,
                batch_id,
            ),
        )
        _append_change(
            conn,
            batch_id=batch_id,
            port_id=port.id,
            change_type="upsert",
            event_id=cursor.lastrowid,
            stay=stay,
            created_at=created_at,
            operation=operation,
        )


def _recompute_vessels(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    port: Port,
    vessel_ids: Iterable[str],
    created_at: str,
    operation: str = "ingest",
) -> None:
    """Re-derive events for the named vessels from their stored points."""
    ordered = sorted(set(vessel_ids))
    if not ordered:
        return
    points = _load_points_for_vessels(conn, port.id, ordered)
    existing_events = _existing_events(conn, port.id, ordered)
    for vessel_id in ordered:
        _reconcile_vessel(
            conn,
            batch_id=batch_id,
            port=port,
            vessel_id=vessel_id,
            points=points[vessel_id],
            existing=existing_events[vessel_id],
            created_at=created_at,
            operation=operation,
        )


def ingest(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    upload_key: str,
    content: bytes,
    rows: list[RawPointRow],
    rows_received: int | None = None,
    in_file_duplicates: int = 0,
) -> tuple[dict[str, Any], bool]:
    """Apply an upload atomically.  Returns (response, replayed)."""
    content_sha = hashlib.sha256(content).hexdigest()

    with _transaction_if_needed(conn):
        port = get_port(conn, port_code)

        previous = conn.execute(
            "SELECT * FROM ingest_batches WHERE port_id = ? AND upload_key = ?",
            (port.id, upload_key),
        ).fetchone()
        if previous is not None:
            if previous["content_sha256"] != content_sha:
                raise AppError(
                    409,
                    "upload_key_reused",
                    (
                        f"upload_key {upload_key!r} was already used for "
                        "different content"
                    ),
                )
            # A revoked batch is never revived by replaying its key: report the
            # original batch together with its revocation state.
            return _batch_response(conn, previous["id"]), True

        created_at = utc_now_iso()
        batch_cursor = conn.execute(
            """
            INSERT INTO ingest_batches
                (port_id, upload_key, content_sha256, rows_received, rows_new,
                 rows_duplicate, rows_outside_port, received_at,
                 provenance_version)
            VALUES (?, ?, ?, ?, 0, 0, 0, ?, ?)
            """,
            (port.id, upload_key, content_sha,
             rows_received if rows_received is not None else len(rows),
             created_at, PROVENANCE_VERSION),
        )
        batch_id = int(batch_cursor.lastrowid)

        new_by_vessel: set[str] = set()
        duplicates = in_file_duplicates
        outside = 0
        inserted = 0
        for row in rows:
            # Out-of-port points are stored as raw observations too; they act
            # as exit markers and make later reprocessing deterministic.
            if not port.contains(row.lat_e6, row.lon_e6):
                outside += 1
            existing = conn.execute(
                """
                SELECT id, lat_e6, lon_e6
                FROM raw_points
                WHERE port_id = ? AND vessel_id = ? AND event_time = ?
                """,
                (port.id, row.vessel_id, row.event_time),
            ).fetchone()
            if existing is not None:
                if (existing["lat_e6"], existing["lon_e6"]) != (
                    row.lat_e6,
                    row.lon_e6,
                ):
                    raise AppError(
                        409,
                        "conflicting_coordinates",
                        (
                            f"stored point for vessel {row.vessel_id!r} at "
                            f"{row.event_time} has different coordinates"
                        ),
                        {
                            "vessel": row.vessel_id,
                            "time": row.event_time,
                            "row": row.row_number,
                        },
                    )
                duplicates += 1
                point_id = int(existing["id"])
            else:
                point_cursor = conn.execute(
                    """
                    INSERT INTO raw_points
                        (port_id, batch_id, vessel_id, event_time, lat_e6, lon_e6)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        port.id,
                        batch_id,
                        row.vessel_id,
                        row.event_time,
                        row.lat_e6,
                        row.lon_e6,
                    ),
                )
                point_id = int(point_cursor.lastrowid)
                new_by_vessel.add(row.vessel_id)
                inserted += 1
            # Provenance is recorded even for a duplicate point: this batch
            # also asserts that observation, so revoking another source that
            # first stored it must not delete the point.
            conn.execute(
                """
                INSERT INTO point_sources (point_id, batch_id, created_at)
                VALUES (?, ?, ?)
                """,
                (point_id, batch_id, created_at),
            )

        conn.execute(
            """
            UPDATE ingest_batches
            SET rows_new = ?, rows_duplicate = ?, rows_outside_port = ?
            WHERE id = ?
            """,
            (inserted, duplicates, outside, batch_id),
        )

        _recompute_vessels(
            conn,
            batch_id=batch_id,
            port=port,
            vessel_ids=new_by_vessel,
            created_at=created_at,
        )

        return _batch_response(conn, batch_id), False


def revoke_batch(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    batch_id: int,
) -> tuple[dict[str, Any], bool]:
    """Revoke one upload atomically.

    Returns ``(response, replayed)``.  Replaying a revocation returns the
    original revocation and emits no changes.  Only points with no remaining
    live source are deleted; events are then recomputed for every affected
    vessel.  Unchanged events keep their id, changed ones are retracted under
    the old id and re-inserted under a new id, exactly like a late upload.
    """
    with _transaction_if_needed(conn):
        port = get_port(conn, port_code)
        batch = conn.execute(
            "SELECT * FROM ingest_batches WHERE id = ? AND port_id = ?",
            (batch_id, port.id),
        ).fetchone()
        if batch is None:
            raise AppError(
                404,
                "batch_not_found",
                f"batch {batch_id} does not exist in port {port.code!r}",
            )
        if batch["provenance_version"] < PROVENANCE_VERSION:
            raise AppError(
                409,
                "batch_provenance_incomplete",
                (
                    f"batch {batch_id} predates per-point provenance and "
                    "cannot be revoked safely"
                ),
                {"batch_id": batch_id},
            )
        if batch["revoked_at"] is not None:
            return _revocation_response(conn, batch), True

        created_at = utc_now_iso()
        sourced = conn.execute(
            """
            SELECT point_id FROM point_sources WHERE batch_id = ?
            """,
            (batch_id,),
        ).fetchall()

        affected_vessels: set[str] = set()
        removed = 0
        retained = 0
        for source in sourced:
            point = conn.execute(
                "SELECT * FROM raw_points WHERE id = ?",
                (source["point_id"],),
            ).fetchone()
            if point is None:
                # Already removed with another batch's shared point; the source
                # row cascades away, so this is only defensive.
                continue
            others = conn.execute(
                """
                SELECT 1 FROM point_sources ps
                JOIN ingest_batches b ON b.id = ps.batch_id
                WHERE ps.point_id = ? AND ps.batch_id <> ?
                  AND b.revoked_at IS NULL
                LIMIT 1
                """,
                (point["id"], batch_id),
            ).fetchone()
            if others is not None:
                retained += 1
                continue
            # A point inserted by a legacy (pre-provenance) batch has no
            # explicit source row, but that batch remains an implicit source.
            # Revoking a newer batch that merely retransmitted it must not
            # delete an observation the legacy batch still owns.
            if point["batch_id"] != batch_id:
                owner = conn.execute(
                    "SELECT provenance_version, revoked_at FROM ingest_batches "
                    "WHERE id = ?",
                    (point["batch_id"],),
                ).fetchone()
                if (
                    owner is not None
                    and owner["revoked_at"] is None
                    and owner["provenance_version"] < PROVENANCE_VERSION
                ):
                    retained += 1
                    continue
            conn.execute("DELETE FROM raw_points WHERE id = ?", (point["id"],))
            affected_vessels.add(point["vessel_id"])
            removed += 1

        conn.execute(
            """
            UPDATE ingest_batches
            SET revoked_at = ?, points_removed = ?, points_retained = ?
            WHERE id = ?
            """,
            (created_at, removed, retained, batch_id),
        )

        _recompute_vessels(
            conn,
            batch_id=batch_id,
            port=port,
            vessel_ids=affected_vessels,
            created_at=created_at,
            operation="revoke",
        )

        revoked_batch = conn.execute(
            "SELECT * FROM ingest_batches WHERE id = ?", (batch_id,)
        ).fetchone()
        return _revocation_response(conn, revoked_batch), False


def _revocation_response(
    conn: sqlite3.Connection,
    batch: sqlite3.Row,
) -> dict[str, Any]:
    revoked_at = batch["revoked_at"]
    change_rows = conn.execute(
        "SELECT change_type FROM event_changes "
        "WHERE batch_id = ? AND operation = 'revoke'",
        (batch["id"],),
    ).fetchall()
    first = conn.execute(
        "SELECT MIN(seq) AS s FROM event_changes "
        "WHERE batch_id = ? AND operation = 'revoke'",
        (batch["id"],),
    ).fetchone()["s"]
    last = conn.execute(
        "SELECT MAX(seq) AS s FROM event_changes "
        "WHERE batch_id = ? AND operation = 'revoke'",
        (batch["id"],),
    ).fetchone()["s"]
    return {
        "batch_id": batch["id"],
        "port_id": batch["port_id"],
        "upload_key": batch["upload_key"],
        "revoked": True,
        "revoked_at": revoked_at,
        "points_removed": batch["points_removed"],
        "points_retained": batch["points_retained"],
        "event_changes": {
            "upserted": sum(1 for c in change_rows if c["change_type"] == "upsert"),
            "retracted": sum(
                1 for c in change_rows if c["change_type"] == "retract"
            ),
        },
        "change_cursor": {"from": first, "to": last},
    }


def list_events(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    vessel_id: str | None = None,
    after_event_id: int = 0,
    limit: int = 1000,
) -> tuple[list[dict[str, Any]], bool]:
    """Return (events, has_more), paged by ascending event id."""
    port = get_port(conn, port_code)
    sql = "SELECT * FROM events WHERE port_id = ? AND id > ?"
    args: list[Any] = [port.id, after_event_id]
    if vessel_id is not None:
        sql += " AND vessel_id = ?"
        args.append(vessel_id)
    sql += " ORDER BY id LIMIT ?"
    args.append(limit + 1)
    rows = conn.execute(sql, args).fetchall()
    has_more = len(rows) > limit
    return [_event_dict(r) for r in rows[:limit]], has_more


def _event_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_id": row["id"],
        "port_id": row["port_id"],
        "vessel_id": row["vessel_id"],
        "start_time": row["start_time"],
        "end_time": row["end_time"],
        "duration_seconds": row["end_time"] - row["start_time"],
        "point_count": row["point_count"],
        "start": {"lat": row["start_lat_e6"] / 1e6, "lon": row["start_lon_e6"] / 1e6},
        "end": {"lat": row["end_lat_e6"] / 1e6, "lon": row["end_lon_e6"] / 1e6},
    }


def list_changes(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    after: int,
    limit: int,
) -> dict[str, Any]:
    port = get_port(conn, port_code)
    rows = conn.execute(
        """
        SELECT * FROM event_changes
        WHERE port_id = ? AND seq > ?
        ORDER BY seq
        LIMIT ?
        """,
        (port.id, after, limit + 1),
    ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    changes = [_change_dict(r) for r in rows]
    next_cursor = rows[-1]["seq"] if rows else after
    return {
        "port_id": port.id,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "changes": changes,
    }


def _change_dict(row: sqlite3.Row) -> dict[str, Any]:
    change: dict[str, Any] = {
        "cursor": row["seq"],
        "batch_id": row["batch_id"],
        "operation": row["operation"],
        "type": row["change_type"],
        "event_id": row["event_id"],
        "vessel_id": row["vessel_id"],
        "start_time": row["start_time"],
        "end_time": row["end_time"],
        "created_at": row["created_at"],
    }
    if row["change_type"] == "upsert":
        change["event"] = {
            "event_id": row["event_id"],
            "port_id": row["port_id"],
            "vessel_id": row["vessel_id"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "duration_seconds": row["end_time"] - row["start_time"],
            "point_count": row["point_count"],
            "start": {
                "lat": row["start_lat_e6"] / 1e6,
                "lon": row["start_lon_e6"] / 1e6,
            },
            "end": {
                "lat": row["end_lat_e6"] / 1e6,
                "lon": row["end_lon_e6"] / 1e6,
            },
        }
    return change


def latest_cursor(conn: sqlite3.Connection, *, port_code: str) -> int:
    port = get_port(conn, port_code)
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS s FROM event_changes WHERE port_id = ?",
        (port.id,),
    ).fetchone()
    return int(row["s"])
