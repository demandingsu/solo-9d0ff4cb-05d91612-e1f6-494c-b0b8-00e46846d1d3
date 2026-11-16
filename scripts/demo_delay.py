"""End-to-end demo script for a running service.

Usage:
    python scripts/demo_delay.py [BASE_URL]

Prints the baseline events, late upload changes (retract + upsert), final
events, then a batch-revocation demo showing that observations retransmitted by
another batch survive while the revoke only drops unshared points.
"""

from __future__ import annotations

import json
import sys
import urllib.request
import uuid

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")
PORT_CODE = "DEMO-SCRIPT-" + uuid.uuid4().hex[:6].upper()


def request(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read())


def upload(
    rows: list[tuple[str, str, float, float]],
    key: str,
    port: str = PORT_CODE,
) -> dict:
    boundary = "----demo" + uuid.uuid4().hex
    csv_body = "vessel,time,lat,lon\n" + "\n".join(
        f"{vessel},{time},{lat},{lon}" for vessel, time, lat, lon in rows
    ) + "\n"
    payload = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="ais.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
        f"{csv_body}\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    req = urllib.request.Request(
        f"{BASE}/ports/{port}/uploads",
        data=payload,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Idempotency-Key": key,
        },
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read())


def register(code: str) -> None:
    request(
        "POST",
        "/ports",
        {
            "code": code,
            "name": "演示港区",
            "min_lat": 31.0,
            "max_lat": 31.4,
            "min_lon": 121.2,
            "max_lon": 121.8,
        },
    )


def revoke(batch_id: int, port: str) -> dict:
    req = urllib.request.Request(
        f"{BASE}/ports/{port}/uploads/{batch_id}/revoke", method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read())


def _revoke_demo() -> None:
    port = "DEMO-REVOKE-" + uuid.uuid4().hex[:6].upper()
    register(port)
    seed = [
        ("SHIP-A", "2026-01-01T00:00:00Z", 31.20, 121.50),
        ("SHIP-A", "2026-01-01T00:10:00Z", 31.201, 121.501),
        ("SHIP-A", "2026-01-01T00:20:00Z", 31.202, 121.502),
        ("SHIP-A", "2026-01-01T00:30:00Z", 31.203, 121.503),
        ("SHIP-B", "2026-01-01T00:00:00Z", 31.30, 121.60),
        ("SHIP-B", "2026-01-01T00:10:00Z", 31.301, 121.601),
        ("SHIP-B", "2026-01-01T00:20:00Z", 31.302, 121.602),
        ("SHIP-B", "2026-01-01T00:30:00Z", 31.303, 121.603),
    ]
    seed_batch = upload(seed, "revoke-seed", port)
    retransmit = [row for row in seed if row[0] == "SHIP-B"]
    upload(retransmit, "revoke-retransmit", port)

    print("\n=== revoke demo ===")
    print("seed batch:", seed_batch["batch_id"],
          "events:", len(request("GET", f"/ports/{port}/events")["events"]))
    result = revoke(seed_batch["batch_id"], port)
    print("revoke:", json.dumps(
        {k: result[k] for k in
         ("points_removed", "points_retained", "event_changes")}
    ))
    remaining = request("GET", f"/ports/{port}/events")["events"]
    print("remaining vessels:",
          sorted({event["vessel_id"] for event in remaining}))
    again = revoke(seed_batch["batch_id"], port)
    print("repeat revoke replayed:", again["replayed"],
          "| cursor stable:",
          again["change_cursor"] == result["change_cursor"])
    replay = upload(seed, "revoke-seed", port)
    print("replay revoked batch -> revoked:", replay["revoked"],
          "| replayed:", replay["replayed"])


def main() -> None:
    register(PORT_CODE)
    seed = [
        ("SHIP-A", "2026-01-01T00:00:00Z", 31.20, 121.50),
        ("SHIP-A", "2026-01-01T00:10:00Z", 31.201, 121.501),
        ("SHIP-A", "2026-01-01T00:20:00Z", 31.202, 121.502),
        ("SHIP-A", "2026-01-01T00:30:00Z", 31.203, 121.503),
        ("SHIP-A", "2026-01-01T00:40:00Z", 31.204, 121.504),
        ("SHIP-A", "2026-01-01T00:50:00Z", 31.205, 121.505),
        ("SHIP-A", "2026-01-01T01:00:00Z", 31.206, 121.506),
        ("SHIP-A", "2026-01-01T02:00:00Z", 31.21, 121.51),
        ("SHIP-A", "2026-01-01T02:10:00Z", 31.211, 121.511),
        ("SHIP-A", "2026-01-01T02:20:00Z", 31.212, 121.512),
        ("SHIP-A", "2026-01-01T02:30:00Z", 31.213, 121.513),
    ]
    upload(seed, "seed")
    baseline = request("GET", f"/ports/{PORT_CODE}/events")
    cursor = baseline["cursor"]
    print("baseline events:")
    for event in baseline["events"]:
        print(
            f"  id={event['event_id']} {event['start_time']}..{event['end_time']}"
        )

    late = [
        ("SHIP-A", "2025-12-31T23:50:00Z", 31.19, 121.49),
        ("SHIP-A", "2025-12-31T23:55:00Z", 31.191, 121.491),
    ]
    result = upload(late, "late")
    print("\nlate upload:", json.dumps(result["event_changes"]))

    page = request("GET", f"/ports/{PORT_CODE}/changes?after={cursor}")
    print("\nchanges after baseline cursor:")
    for change in page["changes"]:
        print(f"  seq={change['cursor']} {change['type']} event_id={change['event_id']}")

    final = request("GET", f"/ports/{PORT_CODE}/events")
    print("\nfinal events:")
    for event in final["events"]:
        print(
            f"  id={event['event_id']} {event['start_time']}..{event['end_time']}"
        )
    baseline_ids = {e["event_id"] for e in baseline["events"]}
    final_ids = {e["event_id"] for e in final["events"]}
    print("\nretained ids:", sorted(baseline_ids & final_ids))
    print("retracted ids:", sorted(baseline_ids - final_ids))
    print("added ids:", sorted(final_ids - baseline_ids))

    _revoke_demo()


if __name__ == "__main__":
    main()
