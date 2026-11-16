from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import csv_ingest, db, service
from app.config import Settings
from app.main import create_app

PORT = {
    "code": "CNSHA-PD",
    "name": "上海港浦东",
    "min_lat": 31.0,
    "max_lat": 31.4,
    "min_lon": 121.2,
    "max_lon": 121.8,
}
INSIDE = (31.2, 121.5)


def utc(hour: int, minute: int = 0, second: int = 0, day: int = 1) -> str:
    return f"2026-01-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"


def csv_bytes(rows: list[tuple[str, str, float, float]]) -> bytes:
    lines = ["vessel,time,lat,lon"]
    lines += [f"{v},{t},{lat},{lon}" for v, t, lat, lon in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    settings = Settings(
        db_path=str(tmp_path / "test.sqlite3"), demo_enabled=True
    )
    app = create_app(settings)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def registered(client: TestClient) -> TestClient:
    response = client.post("/ports", json=PORT)
    assert response.status_code == 201, response.text
    return client


def upload(
    client: TestClient,
    rows: list[tuple[str, str, float, float]],
    *,
    key: str | None = None,
    port: str = PORT["code"],
    files: bytes | None = None,
):
    headers = {"Idempotency-Key": key} if key is not None else {}
    return client.post(
        f"/ports/{port}/uploads",
        files={"file": ("ais.csv", files or csv_bytes(rows), "text/csv")},
        headers=headers,
    )


def events(client: TestClient, vessel: str | None = None) -> list[dict]:
    params = {"vessel_id": vessel} if vessel else {}
    response = client.get(f"/ports/{PORT['code']}/events", params=params)
    assert response.status_code == 200, response.text
    return response.json()["events"]


def changes(client: TestClient, after: int = 0) -> dict:
    response = client.get(
        f"/ports/{PORT['code']}/changes", params={"after": after, "limit": 1000}
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_port_registration_is_idempotent(client: TestClient) -> None:
    first = client.post("/ports", json=PORT)
    second = client.post("/ports", json=PORT)
    assert first.status_code == 201 and first.json()["created"] is True
    assert second.status_code == 200 and second.json()["created"] is False

    conflicting = client.post("/ports", json={**PORT, "max_lat": 31.5})
    assert conflicting.status_code == 409
    assert conflicting.json()["error"]["code"] == "port_conflict"


def test_invalid_rectangle_rejected(client: TestClient) -> None:
    bad = client.post("/ports", json={**PORT, "min_lat": 31.5})
    assert bad.status_code == 422


def test_stay_requires_ten_minute_gap_and_thirty_minutes(
    registered: TestClient,
) -> None:
    # 00:00 -> 00:30 with 10 min spacing qualifies exactly.
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    response = upload(registered, rows, key="k1")
    assert response.status_code == 201, response.text
    assert response.json()["rows_new"] == 4

    found = events(registered)
    assert len(found) == 1
    assert found[0]["start_time"] == found[0]["start_time"]
    assert found[0]["duration_seconds"] == 1800
    assert found[0]["point_count"] == 4


def test_gap_just_over_ten_minutes_breaks_event(
    registered: TestClient,
) -> None:
    rows = [
        # First qualifying stay: 00:00 -> 00:30 at 10 minute spacing.
        ("SHIP-A", utc(0, 0), *INSIDE),
        ("SHIP-A", utc(0, 10), *INSIDE),
        ("SHIP-A", utc(0, 20), *INSIDE),
        ("SHIP-A", utc(0, 30), *INSIDE),
        # 10 min 1 s gap: must break the event.
        ("SHIP-A", utc(0, 40, 1), *INSIDE),
        ("SHIP-A", utc(0, 50, 1), *INSIDE),
        ("SHIP-A", utc(1, 0, 1), *INSIDE),
        ("SHIP-A", utc(1, 10, 1), *INSIDE),
    ]
    upload(registered, rows, key="k-gap")
    found = events(registered)
    assert len(found) == 2
    assert all(e["duration_seconds"] == 1800 for e in found)


def test_short_stay_not_an_event(registered: TestClient) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 29)]
    upload(registered, rows, key="k-short")
    assert events(registered) == []
    assert changes(registered)["changes"] == []


def test_out_of_port_point_closes_event(registered: TestClient) -> None:
    rows = [
        ("SHIP-A", utc(0, 0), *INSIDE),
        ("SHIP-A", utc(0, 10), *INSIDE),
        ("SHIP-A", utc(0, 20), *INSIDE),
        ("SHIP-A", utc(0, 30), *INSIDE),
        # Exit, then a fresh qualifying stay after it.
        ("SHIP-A", utc(0, 31), 30.5, 120.0),
        ("SHIP-A", utc(0, 41), *INSIDE),
        ("SHIP-A", utc(0, 51), *INSIDE),
        ("SHIP-A", utc(1, 1), *INSIDE),
        ("SHIP-A", utc(1, 11), *INSIDE),
    ]
    upload(registered, rows, key="k-exit")
    found = events(registered)
    assert len(found) == 2
    assert found[0]["end_time"] + 1 <= found[1]["start_time"]


def test_duplicate_points_deduplicated(registered: TestClient) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    first = upload(registered, rows, key="dup-1")
    assert first.status_code == 201
    assert first.json()["rows_new"] == 4
    assert first.json()["change_cursor"] == {"from": 1, "to": 1}

    # A file that repeats its own rows counts both copies consistently.
    repeated = upload(registered, rows[:2] + rows[:1], key="dup-in-file")
    assert repeated.status_code == 201
    # rows[:2] both already stored, and rows[0] repeats inside the file.
    assert repeated.json()["rows_duplicate"] == 3
    assert repeated.json()["rows_new"] == 0

    second = upload(registered, rows, key="dup-2")
    assert second.status_code == 201
    body = second.json()
    assert body["rows_new"] == 0
    assert body["rows_duplicate"] == 4
    assert body["event_changes"] == {"upserted": 0, "retracted": 0}
    assert changes(registered)["changes"][-1]["cursor"] == 1


def test_same_vessel_same_time_different_coords_rejects_whole_batch(
    registered: TestClient,
) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    upload(registered, rows, key="conf-1")
    before_events = events(registered)
    before_changes = changes(registered)["changes"]

    # One valid new point first in row order, then a conflicting coordinate.
    bad_rows = [
        ("SHIP-A", utc(2, 0), *INSIDE),
        ("SHIP-B", utc(2, 0), *INSIDE),
        ("SHIP-B", utc(2, 0), 31.21, 121.51),
    ]
    response = upload(registered, bad_rows, key="conf-2")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "conflicting_coordinates"
    assert events(registered) == before_events
    assert changes(registered)["changes"] == before_changes
    # A rejected batch must not even leave a batch row behind.
    assert client_batch_count(registered) == 1


def client_batch_count(client: TestClient) -> int:
    db_path = client.app.state.settings.db_path  # type: ignore[attr-defined]
    connection = sqlite3.connect(db_path)
    try:
        return connection.execute("SELECT COUNT(*) FROM ingest_batches").fetchone()[0]
    finally:
        connection.close()


def test_stored_conflict_rolls_back_entire_batch(
    registered: TestClient,
) -> None:
    seed = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    upload(registered, seed, key="seed")
    cursor_before = changes(registered)["next_cursor"]
    rows_before = raw_point_count(registered)

    # valid first row for SHIP-B, then a conflict with the stored SHIP-A point
    bad = [
        ("SHIP-B", utc(1, 0), *INSIDE),
        ("SHIP-A", utc(0, 10), 31.3, 121.7),  # same instant, other coords
    ]
    response = upload(registered, bad, key="bad")
    assert response.status_code == 409
    assert changes(registered)["next_cursor"] == cursor_before
    assert raw_point_count(registered) == rows_before
    assert all(e["vessel_id"] != "SHIP-B" for e in events(registered))


def raw_point_count(client: TestClient) -> int:
    db_path = client.app.state.settings.db_path  # type: ignore[attr-defined]
    connection = sqlite3.connect(db_path)
    try:
        return connection.execute("SELECT COUNT(*) FROM raw_points").fetchone()[0]
    finally:
        connection.close()


def test_late_point_retracts_and_recreates_changed_event_only(
    registered: TestClient,
) -> None:
    # Event A 00:00-01:00, Event B 02:00-02:30, Event C 03:00-03:30.
    rows = []
    rows += [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30, 40, 50)]
    rows += [("SHIP-A", utc(1, 0), *INSIDE)]
    rows += [("SHIP-A", utc(2, m), *INSIDE) for m in (0, 10, 20, 30)]
    rows += [("SHIP-A", utc(3, m), *INSIDE) for m in (0, 10, 20, 30)]
    upload(registered, rows, key="base")

    before = {e["start_time"]: e for e in events(registered)}
    assert len(before) == 3
    cursor_before = changes(registered)["next_cursor"]

    # Two late points extend event A's start to 23:45.
    late = [
        ("SHIP-A", "2025-12-31T23:45:00Z", *INSIDE),
        ("SHIP-A", "2025-12-31T23:55:00Z", *INSIDE),
    ]
    response = upload(registered, late, key="late")
    assert response.status_code == 201

    after = {e["start_time"]: e for e in events(registered)}
    assert len(after) == 3

    new_changes = changes(registered, after=cursor_before)
    kinds = [(c["type"], c["event_id"]) for c in new_changes["changes"]]
    assert [kind for kind, _ in kinds] == ["retract", "upsert"]

    old_a = before[int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())]
    new_a = after[
        int(datetime(2025, 12, 31, 23, 45, tzinfo=timezone.utc).timestamp())
    ]
    assert kinds[0][1] == old_a["event_id"]
    assert kinds[1][1] == new_a["event_id"]

    # Event B and C keep their ids and payloads.
    for start in list(before)[1:]:
        assert after[start]["event_id"] == before[start]["event_id"]
        assert after[start] == before[start]

    assert changes(registered, after=cursor_before)["next_cursor"] == (
        new_changes["next_cursor"]
    )


def test_late_point_can_merge_two_events(registered: TestClient) -> None:
    # Two 30-minute stays separated by a 20 minute gap (00:30 -> 00:50).
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    rows += [("SHIP-A", utc(0, 50), *INSIDE)]
    rows += [("SHIP-A", utc(1, 0), *INSIDE)]
    rows += [("SHIP-A", utc(1, 10), *INSIDE)]
    rows += [("SHIP-A", utc(1, 20), *INSIDE)]
    upload(registered, rows, key="base2")
    assert len(events(registered)) == 2
    cursor_before = changes(registered)["next_cursor"]

    # A point at 00:40 bridges both 10 minute gaps.
    upload(registered, [("SHIP-A", utc(0, 40), *INSIDE)], key="bridge")
    merged = events(registered)
    assert len(merged) == 1
    assert merged[0]["point_count"] == 9

    page = changes(registered, after=cursor_before)
    assert [c["type"] for c in page["changes"]] == ["retract", "retract", "upsert"]


def test_idempotent_replay_does_not_emit_changes(registered: TestClient) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    first = upload(registered, rows, key="same-key")
    assert first.status_code == 201
    cursor = changes(registered)["next_cursor"]

    replay = upload(registered, rows, key="same-key")
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.json()["batch_id"] == first.json()["batch_id"]
    assert changes(registered)["next_cursor"] == cursor

    # Same key with different content must not overwrite history.
    reuse = upload(registered, rows + [("SHIP-A", utc(1, 0), *INSIDE)],
                   key="same-key")
    assert reuse.status_code == 409
    assert reuse.json()["error"]["code"] == "upload_key_reused"


def test_content_hash_idempotency_without_header(registered: TestClient) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    first = upload(registered, rows)
    replay = upload(registered, rows)
    assert first.status_code == 201
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True


def test_csv_validation_errors_are_whole_batch(registered: TestClient) -> None:
    malformed = (
        b"vessel,time,lat,lon\n"
        b"SHIP-A,2026-01-01T00:00:00Z,31.2,121.5\n"
        b"SHIP-A,not-a-time,31.2,121.5\n"
    )
    response = upload(registered, [], files=malformed)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_row"
    assert response.json()["error"]["details"]["row"] == 3
    assert raw_point_count(registered) == 0

    missing = upload(registered, [], files=b"vessel,time\nSHIP-A,1\n")
    assert missing.status_code == 400
    assert missing.json()["error"]["code"] == "missing_columns"


def test_unknown_port_is_404(registered: TestClient) -> None:
    response = upload(registered, [("SHIP-A", utc(0, 0), *INSIDE)], port="NOPE")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "port_not_found"


def test_change_cursor_pagination(registered: TestClient) -> None:
    # Three separate vessels produce three events in one batch.
    rows = []
    for index, vessel in enumerate(("SHIP-A", "SHIP-B", "SHIP-C")):
        base_hour = index * 2
        rows += [
            (vessel, utc(base_hour, m), *INSIDE) for m in (0, 10, 20, 30)
        ]
    upload(registered, rows, key="page")
    full = changes(registered)
    assert len(full["changes"]) == 3
    assert [c["cursor"] for c in full["changes"]] == sorted(
        c["cursor"] for c in full["changes"]
    )

    first = registered.get(
        f"/ports/{PORT['code']}/changes", params={"after": 0, "limit": 2}
    ).json()
    assert first["has_more"] is True
    assert len(first["changes"]) == 2
    second = registered.get(
        f"/ports/{PORT['code']}/changes",
        params={"after": first["next_cursor"], "limit": 2},
    ).json()
    assert second["has_more"] is False
    assert len(second["changes"]) == 1
    assert second["next_cursor"] == full["next_cursor"]


def test_events_response_cursor_matches_change_stream(
    registered: TestClient,
) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    upload(registered, rows, key="cursor")
    body = registered.get(f"/ports/{PORT['code']}/events").json()
    stream = changes(registered, after=body["cursor"])
    assert stream["changes"] == []


def test_upload_with_non_utf8_or_wrong_extension_is_rejected(
    registered: TestClient,
) -> None:
    bad_encoding = upload(
        registered, [], files=b"vessel,time,lat,lon\nSHIP-A,1,31.2,\xff\n"
    )
    assert bad_encoding.status_code == 400
    assert bad_encoding.json()["error"]["code"] == "invalid_encoding"


def test_rows_outside_port_are_reported_but_stored(registered: TestClient) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    rows.append(("SHIP-A", utc(7, 0), 30.0, 120.0))
    body = upload(registered, rows, key="out").json()
    assert body["rows_outside_port"] == 1
    assert body["rows_new"] == 5
    assert len(events(registered)) == 1


def test_upload_into_one_port_does_not_affect_another(tmp_path: Path) -> None:
    app = create_app(Settings(db_path=str(tmp_path / "multi.sqlite3")))
    with TestClient(app) as client:
        for code, shift in (("PORT-A", 0.0), ("PORT-B", 5.0)):
            response = client.post(
                "/ports",
                json={**PORT, "code": code,
                      "min_lat": PORT["min_lat"] + shift,
                      "max_lat": PORT["max_lat"] + shift},
            )
            assert response.status_code == 201
        for code, shift in (("PORT-A", 0.0), ("PORT-B", 5.0)):
            rows = [
                ("SHIP-A", utc(0, m), INSIDE[0] + shift, INSIDE[1])
                for m in (0, 10, 20, 30)
            ]
            assert upload(client, rows, key="shared", port=code).status_code == 201
        events_a = client.get("/ports/PORT-A/events").json()["events"]
        events_b = client.get("/ports/PORT-B/events").json()["events"]
        assert len(events_a) == 1 and len(events_b) == 1
        assert events_a[0]["event_id"] != events_b[0]["event_id"]
        # The seq is global but each port only sees its own changes; cursors
        # may have gaps and remain strictly increasing and resumable.
        cursor_a = client.get("/ports/PORT-A/changes").json()["next_cursor"]
        cursor_b = client.get("/ports/PORT-B/changes").json()["next_cursor"]
        assert cursor_a != cursor_b
        assert client.get(
            "/ports/PORT-A/changes", params={"after": cursor_a}
        ).json()["changes"] == []
        assert client.get(
            "/ports/PORT-B/changes", params={"after": cursor_b}
        ).json()["changes"] == []
        assert client.get(
            "/ports/PORT-B/changes"
        ).json()["port_id"] != client.get("/ports/PORT-A/changes").json()["port_id"]


def test_demo_endpoint_shows_retract_and_preserved_ids(
    client: TestClient,
) -> None:
    response = client.post("/demo/run")
    assert response.status_code == 200, response.text
    body = response.json()
    summary = body["summary"]
    assert len(summary["retracted_event_ids"]) == 1
    assert len(summary["added_event_ids"]) == 1
    assert len(summary["unchanged_retained_event_ids"]) == 2
    change_types = [
        c["type"] for c in body["late_upload"]["changes_after_cursor"]["changes"]
    ]
    assert change_types == ["retract", "upsert"]
    assert body["final_events"]
    # Demo port is isolated and reusable without disturbing the main port.
    assert client.post("/demo/run").status_code == 200


def test_not_registered_port_blocks_upload(tmp_path: Path) -> None:
    app = create_app(Settings(db_path=str(tmp_path / "x.sqlite3")))
    with TestClient(app) as test_client:
        response = test_client.post(
            "/ports/UNKNOWN/uploads",
            files={"file": ("a.csv", csv_bytes([("S", utc(0), *INSIDE)]), "text/csv")},
        )
        assert response.status_code == 404


def test_dual_instance_interleaved_uploads_do_not_lose_updates(
    tmp_path: Path,
) -> None:
    """Two independent connections (as two containers would have) share a DB.

    Each thread uploads a disjoint late point for the same vessel.  BEGIN
    IMMEDIATE must serialise them so both points survive and the resulting
    event reflects both.
    """
    path = str(tmp_path / "dual.sqlite3")
    db.initialise(path)
    setup = db.connect(path)
    try:
        service.register_port(
            setup,
            code="P",
            name="dual",
            min_lat_e6=31_000_000,
            max_lat_e6=31_400_000,
            min_lon_e6=121_200_000,
            max_lon_e6=121_800_000,
        )
        base_rows = [
            csv_ingest.RawPointRow(i, "SHIP-A", 1_800_000_000 + i * 600,
                                   31_200_000, 121_500_000)
            for i in range(4)
        ]
        service.ingest(
            setup,
            port_code="P",
            upload_key="base",
            content=b"base",
            rows=base_rows,
        )
    finally:
        setup.close()

    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def worker(offset: int, key: str) -> None:
        conn = db.connect(path)
        try:
            row = csv_ingest.RawPointRow(
                2, "SHIP-A", 1_800_000_000 - offset * 600, 31_200_000, 121_500_000
            )
            barrier.wait(timeout=10)
            service.ingest(
                conn,
                port_code="P",
                upload_key=key,
                content=key.encode(),
                rows=[row],
            )
        except BaseException as exc:  # pragma: no cover - surfaced in assertion
            errors.append(exc)
        finally:
            conn.close()

    threads = [
        threading.Thread(target=worker, args=(1, "k1")),
        threading.Thread(target=worker, args=(2, "k2")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors, errors
    conn = db.connect(path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM raw_points WHERE vessel_id='SHIP-A'"
        ).fetchone()[0]
        assert count == 6
        found, _ = service.list_events(conn, port_code="P")
        assert len(found) == 1
        assert found[0]["point_count"] == 6
        batches = conn.execute(
            "SELECT COUNT(*) FROM ingest_batches"
        ).fetchone()[0]
        assert batches == 3
    finally:
        conn.close()


def test_two_service_instances_share_state_and_idempotency(
    tmp_path: Path,
) -> None:
    """Two uvicorn instances on one SQLite file behave as one service.

    The dual-compose setup runs exactly this topology.  Uploads through either
    instance must be visible to the other, and replaying the same idempotency
    key through the *other* instance must not emit new changes.
    """
    db_path = str(tmp_path / "instances.sqlite3")
    settings = Settings(db_path=db_path)
    app_one, app_two = create_app(settings), create_app(settings)
    with TestClient(app_one) as one, TestClient(app_two) as two:
        for client in (one, two):
            assert client.post("/ports", json=PORT).status_code in (200, 201)

        rows_one = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
        first = one.post(
            f"/ports/{PORT['code']}/uploads",
            files={"file": ("a.csv", csv_bytes(rows_one), "text/csv")},
            headers={"Idempotency-Key": "replica-key"},
        )
        assert first.status_code == 201
        assert len(two.get(f"/ports/{PORT['code']}/events").json()["events"]) == 1

        # Replay through the other instance: original batch, no new changes.
        cursor = two.get(f"/ports/{PORT['code']}/changes").json()["next_cursor"]
        replay = two.post(
            f"/ports/{PORT['code']}/uploads",
            files={"file": ("a.csv", csv_bytes(rows_one), "text/csv")},
            headers={"Idempotency-Key": "replica-key"},
        )
        assert replay.status_code == 200
        assert replay.json()["batch_id"] == first.json()["batch_id"]
        assert (
            two.get(f"/ports/{PORT['code']}/changes").json()["next_cursor"]
            == cursor
        )

        # A late point through instance two changes the event seen by instance
        # one: both read the same committed state.
        late = [("SHIP-A", "2025-12-31T23:50:00Z", *INSIDE)]
        assert two.post(
            f"/ports/{PORT['code']}/uploads",
            files={"file": ("late.csv", csv_bytes(late), "text/csv")},
            headers={"Idempotency-Key": "late-key"},
        ).status_code == 201
        seen = one.get(f"/ports/{PORT['code']}/events").json()["events"]
        assert len(seen) == 1
        assert seen[0]["point_count"] == 5


def test_unexpected_failure_mid_ingest_rolls_back_everything(
    registered: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash after raw inserts must not leave points or advance the cursor."""
    from app import service as service_module

    original = service_module._reconcile_vessel

    def boom(*args, **kwargs):
        raise RuntimeError("simulated crash during reconciliation")

    monkeypatch.setattr(service_module, "_reconcile_vessel", boom)
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    with pytest.raises(RuntimeError):
        upload(registered, rows, key="boom")
    monkeypatch.setattr(service_module, "_reconcile_vessel", original)

    assert raw_point_count(registered) == 0
    assert client_batch_count(registered) == 0
    assert events(registered) == []
    assert changes(registered)["next_cursor"] == 0


def test_global_change_cursor_never_reuses_a_seq(registered: TestClient) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    upload(registered, rows, key="seq")
    page = changes(registered)
    seqs = [c["cursor"] for c in page["changes"]]
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs)))
    # A failed upload must not burn or roll back cursor numbers visible to
    # consumers: after a rejected batch the next change still gets a fresh seq.
    upload(registered, [("SHIP-A", utc(0, 10), 31.3, 121.7)], key="conflict")
    upload(registered, [("SHIP-A", utc(5, m), *INSIDE) for m in (0, 10, 20, 30)],
           key="after-failure")
    page = changes(registered)
    assert len(page["changes"]) == 2
    assert page["changes"][1]["cursor"] > page["changes"][0]["cursor"]


def test_late_exit_point_splits_an_event(registered: TestClient) -> None:
    # One 80 minute stay (00:00 -> 01:20, 10 minute spacing).
    rows = [("SHIP-A", utc(h, m), *INSIDE) for h, m in
            ((0, 0), (0, 10), (0, 20), (0, 30), (0, 40), (0, 50),
             (1, 0), (1, 10), (1, 20))]
    upload(registered, rows, key="split-base")
    assert len(events(registered)) == 1
    cursor_before = changes(registered)["next_cursor"]

    # A late out-of-port point at 00:35 breaks the run into 00:00-00:30 and
    # 00:40-01:20, both still qualifying: old id retracted, two new ids added.
    upload(registered, [("SHIP-A", utc(0, 35), 30.5, 120.0)], key="split-exit")
    found = events(registered)
    assert len(found) == 2
    assert [e["duration_seconds"] for e in found] == [1800, 2400]

    page = changes(registered, after=cursor_before)
    assert [c["type"] for c in page["changes"]] == ["retract", "upsert", "upsert"]


# ---------------------------------------------------------------------------
# Batch revocation
# ---------------------------------------------------------------------------


def revoke(client: TestClient, batch_id: int, *, port: str = PORT["code"]):
    return client.post(f"/ports/{port}/uploads/{batch_id}/revoke")


def test_revoke_removes_only_points_without_other_source(
    registered: TestClient,
) -> None:
    # Batch A (baseline): SHIP-01 two events and SHIP-02 one event.
    base = []
    base += [("SHIP-01", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    base += [("SHIP-01", utc(1, m), *INSIDE) for m in (0, 10, 20, 30)]
    base += [("SHIP-02", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    first = upload(registered, base, key="revoke-base")
    assert first.status_code == 201
    base_id = first.json()["batch_id"]
    assert first.json()["revocable"] is True

    # Batch B retransmits SHIP-02's exact observations (同船同刻同坐标).
    retransmit = [("SHIP-02", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    same = upload(registered, retransmit, key="revoke-retransmit")
    assert same.status_code == 201
    assert same.json()["rows_new"] == 0
    assert same.json()["rows_duplicate"] == 4

    before = events(registered)
    cursor_before = changes(registered)["next_cursor"]

    response = revoke(registered, base_id)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "revoked"
    assert body["replayed"] is False
    assert body["revoked"] is True
    assert body["batch_id"] == base_id

    after = events(registered)
    # SHIP-02 survives purely on the retransmission's provenance.
    assert [e["vessel_id"] for e in after] == ["SHIP-02"]
    before_by_vessel = {e["vessel_id"]: e for e in before}
    assert after[0]["event_id"] == before_by_vessel["SHIP-02"]["event_id"]
    assert after[0] == before_by_vessel["SHIP-02"]

    page = changes(registered, after=cursor_before)
    # Both SHIP-01 events retracted, nothing re-added.
    assert [c["type"] for c in page["changes"]] == ["retract", "retract"]
    assert {c["vessel_id"] for c in page["changes"]} == {"SHIP-01"}
    assert all(c["operation"] == "revoke" for c in page["changes"])
    assert all(c["batch_id"] == base_id for c in page["changes"])


def test_revoke_recomputes_changed_event_and_keeps_unchanged_ids(
    registered: TestClient,
) -> None:
    # One 40 minute event (00:00..00:40, 10 min spacing) for SHIP-01 and an
    # independent event for SHIP-02 in the same batch.
    rows = [("SHIP-01", utc(0, m), *INSIDE) for m in (0, 10, 20, 30, 40)]
    rows += [("SHIP-02", utc(2, m), *INSIDE) for m in (0, 10, 20, 30)]
    # A second batch extending SHIP-01 with a later point at 00:50.
    base = upload(registered, rows, key="partial-base")
    base_id = base.json()["batch_id"]
    ext = upload(registered, [("SHIP-01", utc(0, 50), *INSIDE)],
                 key="partial-ext")
    ext_id = ext.json()["batch_id"]

    before = {e["vessel_id"]: e for e in events(registered)}
    assert before["SHIP-01"]["duration_seconds"] == 3000  # 00:00..00:50
    cursor_before = changes(registered)["next_cursor"]

    # Revoking the extension leaves 00:00..00:40: SHIP-01 changes, SHIP-02
    # is unaffected and must stay absent from the change stream.
    assert revoke(registered, ext_id).status_code == 200
    after = {e["vessel_id"]: e for e in events(registered)}
    assert after["SHIP-01"]["duration_seconds"] == 2400
    assert after["SHIP-02"] == before["SHIP-02"]
    kinds = [
        (c["type"], c["vessel_id"])
        for c in changes(registered, after=cursor_before)["changes"]
    ]
    assert kinds == [("retract", "SHIP-01"), ("upsert", "SHIP-01")]

    # Revoking the baseline now removes every remaining point and therefore
    # every event.
    assert revoke(registered, base_id).status_code == 200
    assert events(registered) == []


def test_revoke_is_idempotent_and_emits_no_extra_changes(
    registered: TestClient,
) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    batch_id = upload(registered, rows, key="rev-once").json()["batch_id"]

    first = revoke(registered, batch_id)
    assert first.status_code == 200 and first.json()["replayed"] is False
    cursor = changes(registered)["next_cursor"]
    points = raw_point_count(registered)

    second = revoke(registered, batch_id)
    assert second.status_code == 200
    assert second.json()["replayed"] is True
    assert second.json()["batch_id"] == batch_id
    assert second.json()["revoked_at"] == first.json()["revoked_at"]
    assert changes(registered)["next_cursor"] == cursor
    assert raw_point_count(registered) == points == 0
    assert events(registered) == []


def test_replay_of_revoked_upload_returns_revoked_without_reviving(
    registered: TestClient,
) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    original = upload(registered, rows, key="rev-replay")
    batch_id = original.json()["batch_id"]
    assert revoke(registered, batch_id).status_code == 200
    cursor = changes(registered)["next_cursor"]

    replay = upload(registered, rows, key="rev-replay")
    assert replay.status_code == 200
    body = replay.json()
    assert body["replayed"] is True
    assert body["revoked"] is True
    assert body["batch_id"] == batch_id
    assert body["revoked_at"] is not None
    # No points resurrected, no events re-created, no cursor movement.
    assert raw_point_count(registered) == 0
    assert events(registered) == []
    assert changes(registered)["next_cursor"] == cursor
    assert client_batch_count(registered) == 1


def test_legacy_batch_without_provenance_refuses_revocation(
    registered: TestClient,
) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    batch_id = upload(registered, rows, key="legacy").json()["batch_id"]

    # Simulate a batch written by schema v1: no per-point provenance rows and
    # provenance_version = 0.  Revocation must be refused rather than guessing.
    db_path = registered.app.state.settings.db_path  # type: ignore[attr-defined]
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("DELETE FROM point_sources")
        conn.execute(
            "UPDATE ingest_batches SET provenance_version = 0 WHERE id = ?",
            (batch_id,),
        )
        conn.commit()
    finally:
        conn.close()

    cursor = changes(registered)["next_cursor"]
    response = revoke(registered, batch_id)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "batch_provenance_incomplete"
    # Nothing changed: points, events and cursor all intact.
    assert raw_point_count(registered) == 4
    assert len(events(registered)) == 1
    assert changes(registered)["next_cursor"] == cursor


def test_revoke_unknown_or_foreign_batch_is_404(
    registered: TestClient,
) -> None:
    assert revoke(registered, 999).status_code == 404
    assert revoke(registered, 999).json()["error"]["code"] == "batch_not_found"

    # A batch in another port must not be revocable through this port.
    other = registered.post(
        "/ports",
        json={**PORT, "code": "OTHER", "min_lat": 30.0, "max_lat": 30.4},
    )
    assert other.status_code == 201
    rows = [("SHIP-A", 0.0 + 1_800_000_000, 31.2, 121.5)]
    foreign = upload(registered, rows, key="foreign", port="OTHER")
    assert foreign.status_code == 201
    foreign_id = foreign.json()["batch_id"]
    assert revoke(registered, foreign_id).status_code == 404
    assert revoke(registered, foreign_id, port="NOPE").status_code == 404


def test_revoke_rolls_back_atomically_on_failure(
    registered: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app import service as service_module

    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    batch_id = upload(registered, rows, key="rev-crash").json()["batch_id"]
    baseline_events = events(registered)
    baseline_points = raw_point_count(registered)
    baseline_cursor = changes(registered)["next_cursor"]

    original = service_module._recompute_vessels

    def boom(*args, **kwargs):
        raise RuntimeError("simulated crash during revoke reconciliation")

    monkeypatch.setattr(service_module, "_recompute_vessels", boom)
    with pytest.raises(RuntimeError):
        revoke(registered, batch_id)
    monkeypatch.setattr(service_module, "_recompute_vessels", original)

    assert raw_point_count(registered) == baseline_points
    assert events(registered) == baseline_events
    assert changes(registered)["next_cursor"] == baseline_cursor
    assert client_batch_count(registered) == 1
    # The batch is still revocable after the failed attempt.
    assert revoke(registered, batch_id).status_code == 200
    assert events(registered) == []


def test_interleaved_upload_and_revoke_share_one_consistent_state(
    tmp_path: Path,
) -> None:
    """A retransmission interleaved with a revocation on another connection.

    Whichever writer wins the ``BEGIN IMMEDIATE`` lock, the committed state must
    be consistent: either the retransmission is recorded (and the shared point
    survives the revocation) or the revocation commits first.  A point asserted
    by a live, newly committed batch is never deleted.
    """
    path = str(tmp_path / "revoke-race.sqlite3")
    db.initialise(path)
    setup = db.connect(path)
    try:
        service.register_port(
            setup,
            code="P",
            name="revoke-race",
            min_lat_e6=31_000_000,
            max_lat_e6=31_400_000,
            min_lon_e6=121_200_000,
            max_lon_e6=121_800_000,
        )
        base_rows = [
            csv_ingest.RawPointRow(i, "SHIP-A", 1_800_000_000 + i * 600,
                                   31_200_000, 121_500_000)
            for i in range(4)
        ]
        base, _ = service.ingest(
            setup, port_code="P", upload_key="base", content=b"base",
            rows=base_rows,
        )
    finally:
        setup.close()
    base_id = base["batch_id"]

    barrier = threading.Barrier(2)
    errors: list[BaseException] = []
    results: dict[str, object] = {}

    def do_revoke() -> None:
        conn = db.connect(path)
        try:
            barrier.wait(timeout=10)
            results["revoke"] = service.revoke_batch(
                conn, port_code="P", batch_id=base_id
            )[0]
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)
        finally:
            conn.close()

    def do_retransmit() -> None:
        conn = db.connect(path)
        try:
            barrier.wait(timeout=10)
            rows = [
                csv_ingest.RawPointRow(i, "SHIP-A", 1_800_000_000 + i * 600,
                                       31_200_000, 121_500_000)
                for i in range(4)
            ]
            results["retransmit"] = service.ingest(
                conn, port_code="P", upload_key="retransmit",
                content=b"retransmit", rows=rows,
            )[0]
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=do_revoke),
               threading.Thread(target=do_retransmit)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors, errors
    conn = db.connect(path)
    try:
        points = conn.execute(
            "SELECT COUNT(*) FROM raw_points WHERE vessel_id='SHIP-A'"
        ).fetchone()[0]
        sources = conn.execute("SELECT COUNT(*) FROM point_sources").fetchone()[0]
        revoked = conn.execute(
            "SELECT revoked_at FROM ingest_batches WHERE id = ?", (base_id,)
        ).fetchone()[0]
    finally:
        conn.close()
    # The retransmission committed, so every point has two live/dead sources;
    # the revocation removed only points with no live source left.  No point
    # is lost and the source ledger matches the observations.
    assert revoked is not None
    assert points == 4
    assert sources >= points
    conn = db.connect(path)
    try:
        found, _ = service.list_events(conn, port_code="P")
        assert len(found) == 1
        assert found[0]["point_count"] == 4
    finally:
        conn.close()


def test_revoke_demo_endpoint(client: TestClient) -> None:
    response = client.post("/demo/revoke")
    assert response.status_code == 200, response.text
    body = response.json()
    summary = body["summary"]
    assert summary["retained_retransmitted_vessel"] == "SHIP-03"
    assert set(summary["unchanged_retained_vessels"]) == {"SHIP-03"}
    assert len(summary["retracted_event_ids"]) == 3
    assert len(summary["added_event_ids"]) == 0
    assert body["repeat_revocation"]["replayed"] is True
    assert body["replay_after_revocation"]["revoked"] is True
    assert body["replay_after_revocation"]["cursor_unchanged"] is True
    final_vessels = {e["vessel_id"] for e in body["final_events"]}
    assert final_vessels == {"SHIP-03"}
    # Reusable: a second run creates an isolated demo port.
    assert client.post("/demo/revoke").status_code == 200


def test_revoked_batch_response_reports_its_own_ingest_changes_only(
    registered: TestClient,
) -> None:
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    batch_id = upload(registered, rows, key="own-effects").json()["batch_id"]
    revoke(registered, batch_id)
    # Replaying the revoked batch must still show the original ingest effect,
    # not the revocation's retraction, and must stay revoked.
    replay = upload(registered, rows, key="own-effects").json()
    assert replay["revoked"] is True
    assert replay["event_changes"] == {"upserted": 1, "retracted": 0}
    page = changes(registered)
    assert any(c["operation"] == "revoke" for c in page["changes"])


def test_revoking_new_batch_keeps_point_owned_by_legacy_batch(
    registered: TestClient,
) -> None:
    """A legacy batch's point must not be deleted when a newer retransmission
    batch is revoked."""
    rows = [("SHIP-A", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    legacy_id = upload(registered, rows, key="legacy-owner").json()["batch_id"]
    db_path = registered.app.state.settings.db_path  # type: ignore[attr-defined]
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("DELETE FROM point_sources")
        conn.execute(
            "UPDATE ingest_batches SET provenance_version = 0 WHERE id = ?",
            (legacy_id,),
        )
        conn.commit()
    finally:
        conn.close()

    # New batch only retransmits those observations; it owns no point itself.
    newer = upload(registered, rows, key="newer-retransmit")
    assert newer.status_code == 201
    assert newer.json()["rows_new"] == 0
    assert newer.json()["rows_duplicate"] == 4
    newer_id = newer.json()["batch_id"]

    assert revoke(registered, newer_id).status_code == 200
    # The legacy point and its event remain untouched.
    assert raw_point_count(registered) == 4
    assert len(events(registered)) == 1


def test_existing_v1_database_is_migrated_and_batches_stay_protected(
    tmp_path: Path,
) -> None:
    """Upgrade a schema-v1 database holding data; provenance stays incomplete."""
    path = str(tmp_path / "v1.sqlite3")
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE ports (
                id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT NOT NULL UNIQUE
                COLLATE NOCASE, name TEXT NOT NULL, min_lat_e6 INTEGER NOT NULL,
                max_lat_e6 INTEGER NOT NULL, min_lon_e6 INTEGER NOT NULL,
                max_lon_e6 INTEGER NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE ingest_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                port_id INTEGER NOT NULL, upload_key TEXT NOT NULL,
                content_sha256 TEXT NOT NULL, rows_received INTEGER NOT NULL,
                rows_new INTEGER NOT NULL, rows_duplicate INTEGER NOT NULL,
                rows_outside_port INTEGER NOT NULL, received_at TEXT NOT NULL,
                UNIQUE (port_id, upload_key));
            CREATE TABLE raw_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                port_id INTEGER NOT NULL, batch_id INTEGER NOT NULL,
                vessel_id TEXT NOT NULL, event_time INTEGER NOT NULL,
                lat_e6 INTEGER NOT NULL, lon_e6 INTEGER NOT NULL,
                UNIQUE (port_id, vessel_id, event_time));
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                port_id INTEGER NOT NULL, vessel_id TEXT NOT NULL,
                start_time INTEGER NOT NULL, end_time INTEGER NOT NULL,
                start_lat_e6 INTEGER NOT NULL, start_lon_e6 INTEGER NOT NULL,
                end_lat_e6 INTEGER NOT NULL, end_lon_e6 INTEGER NOT NULL,
                point_count INTEGER NOT NULL,
                created_batch_id INTEGER NOT NULL,
                updated_batch_id INTEGER NOT NULL);
            CREATE TABLE event_changes (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL, port_id INTEGER NOT NULL,
                change_type TEXT NOT NULL, event_id INTEGER NOT NULL,
                vessel_id TEXT NOT NULL, start_time INTEGER NOT NULL,
                end_time INTEGER NOT NULL, start_lat_e6 INTEGER,
                start_lon_e6 INTEGER, end_lat_e6 INTEGER, end_lon_e6 INTEGER,
                point_count INTEGER, created_at TEXT NOT NULL);
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta VALUES ('version', '1');
            INSERT INTO ports VALUES (1, 'P', 'legacy', 31000000, 31400000,
                121200000, 121800000, '2026-01-01T00:00:00+00:00');
            INSERT INTO ingest_batches VALUES (1, 1, 'old', 'sha', 4, 4, 0, 0,
                '2026-01-01T00:00:00+00:00');
            INSERT INTO raw_points VALUES
                (1,1,1,'SHIP-A',1800000000,31200000,121500000),
                (2,1,1,'SHIP-A',1800000060,31200000,121500000),
                (3,1,1,'SHIP-A',1800000120,31200000,121500000),
                (4,1,1,'SHIP-A',1800000180,31200000,121500000);
            INSERT INTO events VALUES
                (1,1,'SHIP-A',1800000000,1800000180,31200000,121500000,
                 31200000,121500000,4,1,1);
            """
        )
    finally:
        conn.close()

    db.initialise(path)
    app = create_app(Settings(db_path=path))
    with TestClient(app) as client:
        # Legacy data is still served.
        assert len(client.get("/ports/P/events").json()["events"]) == 1
        response = client.post("/ports/P/uploads/1/revoke")
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "batch_provenance_incomplete"
        # A new upload in the port is revocable and coexists with legacy data.
        new_rows = [("SHIP-B", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
        upload(client, new_rows, key="post-migration", port="P")
        new_batch = client.get("/ports/P/events").json()["events"]
        assert len(new_batch) == 2
        db_path = path
        conn = sqlite3.connect(db_path)
        try:
            conn.row_factory = sqlite3.Row
            batch = conn.execute(
                "SELECT * FROM ingest_batches ORDER BY id DESC LIMIT 1"
            ).fetchone()
            assert batch["provenance_version"] == 1
        finally:
            conn.close()
        assert client.post(
            f"/ports/P/uploads/{batch['id']}/revoke"
        ).status_code == 200
        remaining = client.get("/ports/P/events").json()["events"]
        assert [e["vessel_id"] for e in remaining] == ["SHIP-A"]


def test_revocation_reports_removed_and_retained_point_counts(
    registered: TestClient,
) -> None:
    base = [("SHIP-01", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    base += [("SHIP-02", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    batch_id = upload(registered, base, key="counts").json()["batch_id"]
    upload(registered, [("SHIP-02", utc(0, m), *INSIDE) for m in (0, 10, 20, 30)],
           key="counts-retransmit")

    body = revoke(registered, batch_id).json()
    assert body["points_removed"] == 4
    assert body["points_retained"] == 4
    assert body["event_changes"] == {"upserted": 0, "retracted": 1}
    assert body["change_cursor"]["from"] is not None
    # Replay reports exactly the same persisted counts.
    replay = revoke(registered, batch_id).json()
    assert replay["points_removed"] == 4
    assert replay["points_retained"] == 4
    assert replay["change_cursor"] == body["change_cursor"]
