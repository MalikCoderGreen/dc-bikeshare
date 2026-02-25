from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def station_validation(df: DataFrame) -> DataFrame:
    validated_df = df.withColumn(
        "is_valid",
        F.when(
            (F.col("start_station_id").isNotNull()) &
            (F.col("end_station_id").isNotNull()), True

        ).otherwise(False)
    )
    return validated_df

def classify_trip_type(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "trip_type",
        F.when(
            (F.col("start_station_id") == F.col("end_station_id")) &
            (F.col("start_station_name") == F.col("end_station_name")),
            F.lit("round_trip")
        ).otherwise(F.lit("one_way"))
    )

    return df

# Haversine distance calculation
def calculate_haversine_dist(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "ride_distance_km",
        F.round(F.acos(
            F.sin(F.radians(F.col("start_lat"))) * F.sin(F.radians(F.col("end_lat"))) +
            F.cos(F.radians(F.col("start_lat"))) * F.cos(F.radians(F.col("end_lat"))) *
            F.cos(F.radians(F.col("end_lng")) - F.radians(F.col("start_lng")))
        ) * 6371, 2)  # Earth's radius in kilometers
    )

    return df

# For round trips, estimate based on duration (avg speed: 12 km/h)
def determine_trip_type(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "ride_distance_km",
        F.when(
            (F.col("trip_type") == "round_trip") | (F.col("ride_distance_km") == 0),
            F.round((F.col("ride_duration_minutes") / 60) * 12, 2)
        ).otherwise(
            F.col("ride_distance_km")
        )
    )

    return df

def _data_quality_flags(df: DataFrame) -> DataFrame:
    df = df.withColumn("_duration_quality_flag",
        F.when(
            (F.col("ride_duration_minutes") < 1) | (F.col("ride_duration_minutes") > 1440),
            F.lit("suspicious_duration")
        ).otherwise(F.lit("valid"))
    )
    df = df.withColumn("_distance_quality_flag",
        F.when(
            F.col("ride_distance_miles") / 1.6 > 50,
            F.lit("suspicious_distance")
        ).otherwise(F.lit("valid"))
    )
    df = df.withColumn("_round_trip_quality_flag",
        F.when(
            (F.col("trip_type") == "round_trip") & (F.col("ride_duration_minutes") < 5),
            F.lit("same_station_short")
        ).otherwise(F.lit("valid"))
    )
    df = df.withColumn("_speed_quality_flag",
        F.when(
            (F.col("ride_distance_miles") / (F.col("ride_duration_minutes") / 60)) > 30,
            F.lit("outlier_speed")
        ).otherwise(F.lit("valid"))
    )
    df = df.withColumn("_station_quality_flag",
        F.when(
            F.col("start_station_name").isNull() | F.col("end_station_name").isNull(),
            F.lit("missing_station_info")
        ).otherwise(F.lit("valid"))
    )

    return df