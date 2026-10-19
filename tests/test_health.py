from datetime import UTC, datetime


def test_health_returns_healthy_with_current_utc_time(client):
    response = client.get("/")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "HEALTHY"
    assert body["current_time"].endswith("Z")
    reported = datetime.fromisoformat(body["current_time"])
    assert abs((datetime.now(UTC) - reported).total_seconds()) < 5
