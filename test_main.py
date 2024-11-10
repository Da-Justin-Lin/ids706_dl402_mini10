import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session for testing
@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.appName("Simple PySpark Test").getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="module")
def sample_data(spark):
    # Sample data that mimics the structure of weight_change_dataset.csv
    data = [
        (1, "young", 5.0),
        (2, "middle-aged", -2.0),
        (3, "young", 3.0),
        (4, "middle-aged", 0.0),
        (5, "senior", 7.0)
    ]
    columns = ["id", "age_group", "weight_change"]
    return spark.createDataFrame(data, schema=columns)

def test_data_transformation(sample_data):
    # Apply the transformation: filter rows where weight_change > 0
    transformed_df = sample_data.filter(col("weight_change") > 0)

    # Collect the results to verify
    results = transformed_df.collect()

    # Expected data after transformation
    expected_data = [
        (1, "young", 5.0),
        (3, "young", 3.0),
        (5, "senior", 7.0)
    ]

    # Assert that the results match the expected data
    assert len(results) == len(expected_data)
    for row, expected_row in zip(results, expected_data):
        assert tuple(row) == expected_row
