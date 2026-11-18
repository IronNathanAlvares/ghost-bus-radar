import pytest

from ghostbus_core.topics import HEARTBEAT, TRIP_UPDATES, VEHICLE_POSITIONS, Topic, all_topics


def test_names_are_unique() -> None:
    names = [t.name for t in all_topics()]
    assert len(names) == len(set(names))


def test_feed_topics_have_dead_letter_topics() -> None:
    names = {t.name for t in all_topics()}
    for topic in (TRIP_UPDATES, VEHICLE_POSITIONS):
        assert f"{topic.name}.dlq" in names


def test_heartbeat_outlives_feed_data() -> None:
    # NO_DATA outcomes are computed from heartbeats, so they must be retained at least as long.
    assert HEARTBEAT.retention_ms >= TRIP_UPDATES.retention_ms


@pytest.mark.parametrize("bad", ["TripUpdates.v1", "gtfsr.trip_updates", "gtfsr..v1", "1gtfsr.v1"])
def test_rejects_badly_named_topics(bad: str) -> None:
    with pytest.raises(ValueError):
        Topic(name=bad, partitions=1, retention_ms=1, description="")
