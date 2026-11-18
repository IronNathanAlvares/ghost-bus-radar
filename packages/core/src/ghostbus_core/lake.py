"""Iceberg catalog configuration for Spark.

Locally and in AWS the catalog speaks the same protocol, the Iceberg REST catalog API:

* ``local``: the Apache ``iceberg-rest-fixture`` container, with data in SeaweedFS (S3 API).
* ``s3tables``: Amazon S3 Tables' own Iceberg REST endpoint, signed with SigV4.

Keeping the protocol identical means the jobs never branch on environment; only this
configuration does. The catalog is always registered in Spark as ``lake``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

CATALOG = "lake"
ICEBERG_FORMAT_VERSION = "2"  # Athena cannot read some v3 tables written by EMR 7.12.


class LakeMode(StrEnum):
    LOCAL = "local"
    S3TABLES = "s3tables"


@dataclass(frozen=True)
class LakeSettings:
    mode: LakeMode
    # local
    rest_uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://warehouse/"
    s3_endpoint: str = "http://seaweedfs:8333"
    # s3tables
    region: str = "eu-north-1"
    table_bucket_arn: str = ""

    def __post_init__(self) -> None:
        if self.mode is LakeMode.S3TABLES and not self.table_bucket_arn.startswith(
            "arn:aws:s3tables:"
        ):
            raise ValueError("s3tables mode needs table_bucket_arn like arn:aws:s3tables:...")

    @classmethod
    def from_env(cls, env: dict[str, str]) -> LakeSettings:
        mode = LakeMode(env.get("LAKE_MODE", LakeMode.LOCAL.value))
        defaults = cls(mode=LakeMode.LOCAL)
        return cls(
            mode=mode,
            rest_uri=env.get("LAKE_REST_URI", defaults.rest_uri),
            warehouse=env.get("LAKE_WAREHOUSE", defaults.warehouse),
            s3_endpoint=env.get("LAKE_S3_ENDPOINT", defaults.s3_endpoint),
            region=env.get("AWS_REGION", defaults.region),
            table_bucket_arn=env.get("LAKE_TABLE_BUCKET_ARN", ""),
        )


def spark_catalog_conf(settings: LakeSettings) -> dict[str, str]:
    """Spark configuration entries that register the ``lake`` Iceberg catalog."""
    prefix = f"spark.sql.catalog.{CATALOG}"
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        # ResolvingFileIO picks the implementation by scheme: S3FileIO for s3:// table data,
        # Hadoop for file:// paths. The Iceberg streaming source writes its offsets into the
        # Spark checkpoint through this FileIO, and local checkpoints are file:// paths.
        f"{prefix}.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
        "spark.sql.defaultCatalog": CATALOG,
    }
    if settings.mode is LakeMode.LOCAL:
        conf |= {
            f"{prefix}.uri": settings.rest_uri,
            f"{prefix}.warehouse": settings.warehouse,
            f"{prefix}.s3.endpoint": settings.s3_endpoint,
            f"{prefix}.s3.path-style-access": "true",
        }
    else:
        conf |= {
            f"{prefix}.uri": f"https://s3tables.{settings.region}.amazonaws.com/iceberg",
            f"{prefix}.warehouse": settings.table_bucket_arn,
            f"{prefix}.rest.sigv4-enabled": "true",
            f"{prefix}.rest.signing-name": "s3tables",
            f"{prefix}.rest.signing-region": settings.region,
            f"{prefix}.rest-metrics-reporting-enabled": "false",
        }
    return conf


def table_properties() -> dict[str, str]:
    """Properties every table is created with."""
    return {
        "format-version": ICEBERG_FORMAT_VERSION,
        "write.parquet.compression-codec": "zstd",
    }
