"""SparkSession construction shared by every job."""

from __future__ import annotations

from pyspark.sql import SparkSession

from ghostbus_core.lake import LakeSettings, spark_catalog_conf

BASE_CONF: dict[str, str] = {
    # Every timestamp is UTC inside the lake; Europe/Dublin only appears at presentation time.
    "spark.sql.session.timeZone": "UTC",
    # Small, steady volumes: the default 200 shuffle partitions would mean 200 tiny files a batch.
    "spark.sql.shuffle.partitions": "6",
    "spark.sql.streaming.stateStore.providerClass": (
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    ),
}


def build_spark(
    app_name: str, lake: LakeSettings, extra: dict[str, str] | None = None
) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in {**BASE_CONF, **spark_catalog_conf(lake), **(extra or {})}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
