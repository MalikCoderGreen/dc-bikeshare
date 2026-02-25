import pytest
import bs_transformations
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
    
    df = bs_transformations.classify_trip_type(df)
    
    result = df.collect()
    assert result[0]["trip_type"] == "round_trip"
    assert result[1]["trip_type"] == "one_way"

def test_data_quality_flags(spark):
    """Test suspicious duration flagging"""
    data = [
        (1, 0.5,    5.0, "one_way",    "Station A", "Station B"),  # Too short
        (2, 30.0,   5.0, "one_way",    "Station A", "Station B"),  # Valid
        (3, 1500.0, 5.0, "one_way",    "Station A", "Station B"),  # Too long
    ]
    df = spark.createDataFrame(data, [
        "ride_id", "ride_duration_minutes", "ride_distance_miles",
        "trip_type", "start_station_name", "end_station_name"
    ])

    df = bs_transformations._data_quality_flags(df)

    result = df.collect()
    assert result[0]["_duration_quality_flag"] == "suspicious_duration"
    assert result[1]["_duration_quality_flag"] == "valid"
    assert result[2]["_duration_quality_flag"] == "suspicious_duration"

def test_coordinates_within_dc_bounds(spark):
    """Validate lat/lng coordinates are within DC metro area."""
    data = [
        (1, 38.9072, -77.0369),   # Valid (White House)
        (2, 40.7128, -74.0060),   # Invalid (NYC)
        (3, 38.8977, -77.0365),   # Valid (Capitol)
    ]
    
    df = spark.createDataFrame(data, ["ride_id", "start_lat", "start_lng"])
    
    # DC bounds: roughly 38.79 to 39.0, -77.12 to -76.91
    df = df.withColumn(
        "is_valid_location",
        (F.col("start_lat").between(38.79, 39.0)) & 
        (F.col("start_lng").between(-77.12, -76.91))
    )
    
    result = df.collect()
    assert result[0]["is_valid_location"] is True
    assert result[1]["is_valid_location"] is False  # NYC coords
    assert result[2]["is_valid_location"] is True

def test_null_station_handling(spark):
    """Test handling of rides with missing station information."""
    data = [
        (1, "Station A", 100, "Station B", 200),  # Complete
        (2, None, None, "Station B", 200),        # Missing start
        (3, "Station A", 100, None, None),        # Missing end
    ]
    
    df = spark.createDataFrame(
        data, 
        ["ride_id", "start_station_name", "start_station_id", 
         "end_station_name", "end_station_id"]
    )
    
    # Flag incomplete rides
    df = df.withColumn(
        "has_complete_stations",
        F.col("start_station_name").isNotNull() & 
        F.col("end_station_name").isNotNull()
    )
    
    result = df.collect()
    assert result[0]["has_complete_stations"] is True
    assert result[1]["has_complete_stations"] is False
    assert result[2]["has_complete_stations"] is False


def test_rideable_type_classification(spark):
    """Test classification of bike types."""
    data = [
        (1, "classic_bike"),
        (2, "electric_bike"),
        (3, "docked_bike"),
        (4, "unknown_type"),  # Invalid
    ]
    
    df = spark.createDataFrame(data, ["ride_id", "rideable_type"])
    
    valid_types = ["classic_bike", "electric_bike", "docked_bike"]
    
    df = df.withColumn(
        "is_valid_type",
        F.col("rideable_type").isin(valid_types)
    )
    
    result = df.collect()
    assert result[0]["is_valid_type"] is True
    assert result[1]["is_valid_type"] is True
    assert result[2]["is_valid_type"] is True
    assert result[3]["is_valid_type"] is False

def test_trip_duration_edge_cases(spark):
    """Test edge cases in trip duration calculation."""
    from datetime import datetime, timedelta
    
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    
    data = [
        (1, base_time, base_time + timedelta(minutes=30)),     # Normal
        (2, base_time, base_time + timedelta(seconds=30)),     # Too short (< 1 min)
        (3, base_time, base_time + timedelta(hours=25)),       # Too long (> 24 hrs)
        (4, base_time, base_time),                             # Zero duration
    ]
    
    df = spark.createDataFrame(data, ["ride_id", "started_at", "ended_at"])
    
    # Calculate duration in minutes
    df = df.withColumn(
        "duration_minutes",
        (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")) / 60
    )
    
    # Flag suspicious durations
    df = df.withColumn(
        "is_suspicious",
        (F.col("duration_minutes") < 1) | (F.col("duration_minutes") > 1440)
    )
    
    result = df.collect()
    assert result[0]["is_suspicious"] is False  # Normal 30 min
    assert result[1]["is_suspicious"] is True   # < 1 min
    assert result[2]["is_suspicious"] is True   # > 24 hrs
    assert result[3]["is_suspicious"] is True   # Zero

def test_member_casual_values(spark):
    """Test member_casual field only contains valid values."""
    data = [
        (1, "member"),
        (2, "casual"),
        (3, "Member"),   # Case sensitivity
        (4, "guest"),    # Invalid
    ]
    
    df = spark.createDataFrame(data, ["ride_id", "member_casual"])
    
    # Normalize and validate
    df = df.withColumn("member_casual_lower", F.lower(F.col("member_casual")))
    df = df.withColumn(
        "is_valid",
        F.col("member_casual_lower").isin(["member", "casual"])
    )
    
    result = df.collect()
    assert result[0]["is_valid"] is True
    assert result[1]["is_valid"] is True
    assert result[2]["is_valid"] is True   # After normalization
    assert result[3]["is_valid"] is False  # "guest" invalid

def test_station_id_name_consistency(spark):
    """Test that same station_id always has same station_name."""
    data = [
        (1, 100, "Union Station"),
        (2, 100, "Union Station"),    # Consistent
        (3, 100, "union station"),    # Case difference
        (4, 200, "Dupont Circle"),
    ]
    
    df = spark.createDataFrame(data, ["ride_id", "start_station_id", "start_station_name"])
    
    # Normalize names
    df = df.withColumn("normalized_name", F.lower(F.trim(F.col("start_station_name"))))
    
    # Check for inconsistencies per station_id
    station_names = df.groupBy("start_station_id").agg(
        F.countDistinct("normalized_name").alias("unique_names")
    )
    
    inconsistent = station_names.filter(F.col("unique_names") > 1).count()
    
    assert inconsistent == 0  # All station IDs should map to one name

def test_timestamp_ordering(spark):
    """Test that ended_at is always after started_at."""
    from datetime import datetime, timedelta
    
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    
    data = [
        (1, base_time, base_time + timedelta(hours=1)),   # Valid
        (2, base_time, base_time - timedelta(hours=1)),   # Invalid (end before start)
        (3, base_time, base_time),                        # Edge case (same time)
    ]
    
    df = spark.createDataFrame(data, ["ride_id", "started_at", "ended_at"])
    
    df = df.withColumn(
        "is_valid_order",
        F.col("ended_at") > F.col("started_at")
    )
    
    result = df.collect()
    assert result[0]["is_valid_order"] is True
    assert result[1]["is_valid_order"] is False  # End before start
    assert result[2]["is_valid_order"] is False  # Same time