from pyspark.sql import SparkSession

import pytest


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession
        .builder
        .master("local[1]")
        .appName("truata")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
