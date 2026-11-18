import pytest

from ghostbus_core.lake import LakeMode, LakeSettings, spark_catalog_conf, table_properties

ARN = "arn:aws:s3tables:eu-north-1:123456789012:bucket/ghostbus"


def test_local_mode_points_at_rest_fixture_and_seaweedfs() -> None:
    conf = spark_catalog_conf(LakeSettings(mode=LakeMode.LOCAL))
    assert conf["spark.sql.catalog.lake.type"] == "rest"
    assert conf["spark.sql.catalog.lake.uri"] == "http://iceberg-rest:8181"
    assert conf["spark.sql.catalog.lake.s3.path-style-access"] == "true"
    assert not any("sigv4" in key for key in conf)


def test_s3tables_mode_signs_requests_for_s3tables() -> None:
    conf = spark_catalog_conf(LakeSettings(mode=LakeMode.S3TABLES, table_bucket_arn=ARN))
    assert conf["spark.sql.catalog.lake.uri"] == "https://s3tables.eu-north-1.amazonaws.com/iceberg"
    assert conf["spark.sql.catalog.lake.warehouse"] == ARN
    assert conf["spark.sql.catalog.lake.rest.sigv4-enabled"] == "true"
    assert conf["spark.sql.catalog.lake.rest.signing-name"] == "s3tables"
    assert "spark.sql.catalog.lake.s3.endpoint" not in conf


def test_both_modes_use_the_same_catalog_protocol() -> None:
    local = spark_catalog_conf(LakeSettings(mode=LakeMode.LOCAL))
    cloud = spark_catalog_conf(LakeSettings(mode=LakeMode.S3TABLES, table_bucket_arn=ARN))
    for key in ("spark.sql.catalog.lake", "spark.sql.catalog.lake.type", "spark.sql.extensions"):
        assert local[key] == cloud[key]


def test_s3tables_mode_requires_bucket_arn() -> None:
    with pytest.raises(ValueError):
        LakeSettings(mode=LakeMode.S3TABLES)


def test_from_env_defaults_to_local() -> None:
    assert LakeSettings.from_env({}).mode is LakeMode.LOCAL


def test_from_env_reads_s3tables() -> None:
    settings = LakeSettings.from_env(
        {"LAKE_MODE": "s3tables", "LAKE_TABLE_BUCKET_ARN": ARN, "AWS_REGION": "eu-north-1"}
    )
    assert settings.mode is LakeMode.S3TABLES
    assert settings.table_bucket_arn == ARN


def test_tables_are_iceberg_v2_for_athena() -> None:
    assert table_properties()["format-version"] == "2"


def test_file_io_resolves_by_scheme() -> None:
    """Streaming checkpoints are file:// locally; table data is s3://. One FileIO must do both."""
    conf = spark_catalog_conf(LakeSettings(mode=LakeMode.LOCAL))
    assert conf["spark.sql.catalog.lake.io-impl"] == "org.apache.iceberg.io.ResolvingFileIO"
