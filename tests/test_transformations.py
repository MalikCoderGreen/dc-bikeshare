import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("test").getOrCreate()

def test_trip_type_classification(spark):
    """Test round trip vs one-way classification logic"""
    data = [
        (1, 100, "Station A", 100, "Station A"),  # Round trip
        (2, 100, "Station A", 200, "Station B"),  # One way
    ]
    df = spark.createDataFrame(data, ["ride_id", "start_station_id", "start_station_name", 
                                        "end_station_id", "end_station_name"])
    
    # Apply transformation (from your silver logic)
    df = df.withColumn(
        "trip_type",
        F.when(
            (F.col("start_station_id") == F.col("end_station_id")),
            F.lit("round_trip")
        ).otherwise(F.lit("one_way"))
    )
    
    result = df.collect()
    assert result[0]["trip_type"] == "round_trip"
    assert result[1]["trip_type"] == "one_way"

def test_data_quality_flags(spark):
    """Test suspicious duration flagging"""
    data = [
        (1, 0.5),   # Too short
        (2, 30.0),  # Valid
        (3, 1500.0) # Too long
    ]
    df = spark.createDataFrame(data, ["ride_id", "ride_duration_minutes"])
    
    df = df.withColumn(
        "_data_quality_flag",
        F.when(
            (F.col("ride_duration_minutes") < 1) | (F.col("ride_duration_minutes") > 1440),
            F.lit("suspicious_duration")
        ).otherwise(F.lit("valid"))
    )
    
    result = df.collect()
    assert result[0]["_data_quality_flag"] == "suspicious_duration"
    assert result[1]["_data_quality_flag"] == "valid"
    assert result[2]["_data_quality_flag"] == "suspicious_duration"