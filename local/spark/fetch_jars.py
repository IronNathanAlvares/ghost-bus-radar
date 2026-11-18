"""Resolve Spark connector jars once, at image build time, and bake them into pyspark/jars.

Runtime then needs no network and no ``spark.jars.packages``. Jars that pyspark already ships
(for example hadoop-client-runtime) are skipped so there is never two versions on the classpath.
Versions match EMR 7.12 (Spark 3.5, Iceberg 1.10) so local behaviour matches the cloud.
"""

import re
import shutil
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
]
IVY = Path("/tmp/ivy")
TARGET = Path(pyspark.__file__).parent / "jars"
ARTIFACT = re.compile(r"^(.*?)-\d[\w.\-]*\.jar$")


def artifact(jar_name: str) -> str:
    match = ARTIFACT.match(jar_name)
    return match.group(1) if match else jar_name


spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.jars.packages", ",".join(PACKAGES))
    .config("spark.jars.ivy", str(IVY))
    .getOrCreate()
)
spark.stop()

shipped = {artifact(p.name) for p in TARGET.glob("*.jar")}
for jar in sorted((IVY / "jars").glob("*.jar")):
    name = jar.name.split("_", 1)[1]  # ivy names jars group_artifact-version.jar
    if artifact(name) in shipped:
        print(f"skip (already shipped): {name}")
        continue
    shutil.copy(jar, TARGET / name)
    print(f"added: {name}")
shutil.rmtree(IVY)
