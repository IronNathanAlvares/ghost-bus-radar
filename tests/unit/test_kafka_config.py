from ghostbus_ingest.kafka_config import bootstrap_from_env, producer_config


def test_producer_is_idempotent_with_acks_all() -> None:
    conf = producer_config("kafka:19092", "test")
    assert conf["enable.idempotence"] is True
    assert conf["acks"] == "all"
    # Idempotence only preserves ordering with at most 5 in-flight requests.
    assert int(conf["max.in.flight.requests.per.connection"]) <= 5


def test_bootstrap_defaults_to_host_listener() -> None:
    assert bootstrap_from_env({}) == "localhost:9092"
    assert bootstrap_from_env({"KAFKA_BOOTSTRAP": "kafka:19092"}) == "kafka:19092"
