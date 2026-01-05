import polars as pl

from utils.data_io import DataIO


class TaxiPipeline:
    def launch(self):
        # Initialize DataIO with input and output paths
        data_io = DataIO(input_path="./data/", output_path="./output/")

        # Load the taxi trip data
        tripdata = data_io.load_data(file_name="yellow_tripdata_2025-06.parquet")

        # Load taxi vendors data
        vendors = data_io.load_data(file_name="taxi_vendors.csv")

        # Load taxi zone lookup table
        zones = data_io.load_data(file_name="taxi_zone_lookup.csv")

        # Convert data to lazy frames for processing
        tripdata_lazy = tripdata.lazy()
        vendors_lazy = vendors.lazy()
        zones_lazy = zones.lazy()

        # Remove rows with missing passenger count or non-positive passenger count
        tripdata_lazy = tripdata_lazy.filter(
            pl.col("passenger_count").is_not_null()
        ).filter(pl.col("passenger_count") > 0)

        # Remove rows with missing fare amount or non-positive fare amount
        tripdata_lazy = tripdata_lazy.filter(
            pl.col("fare_amount").is_not_null()
        ).filter(pl.col("fare_amount") > 0)

        # Remove rows with zero trip distance
        tripdata_lazy = tripdata_lazy.filter(pl.col("trip_distance") > 0)

        # Localize datetime columns to New York timezone
        tripdata_lazy = tripdata_lazy.with_columns(
            pl.col("tpep_pickup_datetime")
            .dt.replace_time_zone("America/New_York")
            .alias("tpep_pickup_datetime_est"),
            pl.col("tpep_dropoff_datetime")
            .dt.replace_time_zone("America/New_York")
            .alias("tpep_dropoff_datetime_est"),
        )

        # Remove trip ends that occur before or at the time the trip starts
        tripdata_lazy = tripdata_lazy.filter(
            pl.col("tpep_dropoff_datetime_est") > pl.col("tpep_pickup_datetime_est")
        )

        # Join vendor information
        tripdata_lazy = tripdata_lazy.join(vendors_lazy, on="VendorID", how="left")

        # Join zone lookup on pickup location ID and drop off location ID and filter out invalid zones
        tripdata_lazy = (
            tripdata_lazy.join(
                zones_lazy.select(
                    [
                        pl.col("LocationID").alias("PULocationID"),
                        pl.col("Borough").alias("PU_Borough"),
                        pl.col("Zone").alias("PU_Zone"),
                        pl.col("service_zone").alias("PU_Service_Zone"),
                    ]
                ),
                on="PULocationID",
                how="left",
            )
            .join(
                zones_lazy.select(
                    [
                        pl.col("LocationID").alias("DOLocationID"),
                        pl.col("Borough").alias("DO_Borough"),
                        pl.col("Zone").alias("DO_Zone"),
                        pl.col("service_zone").alias("DO_Service_Zone"),
                    ]
                ),
                on="DOLocationID",
                how="left",
            )
            .filter(pl.col("PU_Service_Zone") != "N/A")
            .filter(pl.col("PU_Service_Zone").is_not_null())
        )

        # Add columns for fare duration in minutes and fare per distance
        tripdata_lazy = tripdata_lazy.with_columns(
            (
                (
                    pl.col("tpep_dropoff_datetime_est")
                    - pl.col("tpep_pickup_datetime_est")
                ).dt.total_milliseconds(fractional=True)
            ).alias("trip_duration"),
            (pl.col("fare_amount") / pl.col("trip_distance")).alias(
                "fare_per_distance"
            ),
        )

        # Create dataframe of aggregated metrics (average fare per distance, average trip duration, trip count) per vendor and pickup/dropoff zone combination
        enriched = tripdata_lazy.group_by(
            ["VendorID", "VendorName", "PU_Borough", "PU_Zone", "DO_Zone"]
        ).agg(
            pl.col("fare_per_distance").mean().alias("avg_fare_per_distance"),
            pl.col("trip_duration").mean().alias("avg_trip_duration_ms"),
            pl.len().alias("num_trips"),
        )

        # Create a data frame showing, for each route (pickup zone to dropoff zone), the vendor name with the lowest average fare per distance, the total number of trips making up the average, as well as the average time travelled for the trips on that route
        df = enriched.select(
            pl.col("PU_Zone")
            .head(1)
            .over(partition_by=["DO_Zone", "PU_Zone"], mapping_strategy="explode")
            .alias("pick_up_zone"),
            pl.col("DO_Zone")
            .head(1)
            .over(partition_by=["DO_Zone", "PU_Zone"], mapping_strategy="explode")
            .alias("drop_off_zone"),
            pl.col("VendorName")
            .sort_by(pl.col("avg_fare_per_distance"))
            .head(1)
            .over(partition_by=["DO_Zone", "PU_Zone"], mapping_strategy="explode")
            .alias("vendor_with_lowest_avg_fare_per_distance"),
            pl.col("avg_fare_per_distance")
            .round(2)
            .sort_by(pl.col("avg_fare_per_distance"))
            .head(1)
            .over(partition_by=["DO_Zone", "PU_Zone"], mapping_strategy="explode")
            .alias("lowest_avg_fare_per_distance"),
            pl.col("VendorName")
            .sort_by("avg_trip_duration_ms")
            .head(1)
            .over(partition_by=["DO_Zone", "PU_Zone"], mapping_strategy="explode")
            .alias("vendor_with_lowest_avg_trip_duration"),
            (pl.col("avg_trip_duration_ms") / 60000)
            .round(2)
            .sort_by("avg_trip_duration_ms")
            .head(1)
            .over(partition_by=["DO_Zone", "PU_Zone"], mapping_strategy="explode")
            .alias("lowest_avg_trip_duration_minutes"),
            pl.col("VendorName")
            .sort_by(pl.col("num_trips"), descending=True)
            .head(1)
            .over(partition_by=["DO_Zone", "PU_Zone"], mapping_strategy="explode")
            .alias("vendor_with_most_trips"),
            pl.col("num_trips")
            .sort_by(pl.col("num_trips"), descending=True)
            .head(1)
            .over(partition_by=["DO_Zone", "PU_Zone"], mapping_strategy="explode")
            .alias("most_trips"),
        )

        # Saving dataframe to output
        data_io.save_data(df.collect(), file_name="taxi_report.parquet")


def entrypoint():
    pipeline = TaxiPipeline()
    pipeline.launch()


if __name__ == "__main__":
    entrypoint()
