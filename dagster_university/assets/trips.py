import duckdb
import os

import requests
from . import constants
from dagster import asset
from datetime import datetime, timedelta
import pandas as pd
from dagster_duckdb import DuckDBResource
from ..partitions import monthly_partition


@asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context):
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]
    # month_to_fetch = '2023-03'

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)


@asset
def taxi_zones_file():
    """
      The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get(
        f"https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )
    with open(constants.TAXI_ZONES_FILE_PATH, 'wb') as output_file:
        output_file.write(raw_taxi_zones.content)


@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition
)
def taxi_trips(context, database: DuckDBResource):
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]

    query = f"""
      create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
      );

      delete from trips where partition_date = '{month_to_fetch}';

      insert into trips
      select
        VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
        tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
      from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)


@asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource):
    sql_query = """
        create or replace table zones as (
          select
            LocationID AS zone_id,
            zone,
            borough,
            the_geom AS geometry
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """
    with database.get_connection() as conn:
        conn.execute(sql_query)


@asset(deps=["taxi_trips"])
def trips_by_week(database: DuckDBResource):
	# conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))

	current_date = datetime.strptime("2023-03-01", constants.DATE_FORMAT)
	end_date = datetime.strptime("2023-04-01", constants.DATE_FORMAT)

	result = pd.DataFrame()
     
	while current_date < end_date:
		current_date_str = current_date.strftime(constants.DATE_FORMAT)
		query = f"""
			select
				vendor_id, total_amount, trip_distance, passenger_count
			from trips
			where date_trunc('week', pickup_datetime) = date_trunc('week', '{current_date_str}'::date)
		"""

		# data_for_week = conn.execute(query).fetch_df()
		with database.get_connection() as conn:
			data_for_week = conn.execute(query).fetch_df()

		aggregate = data_for_week.agg({
			"vendor_id": "count",
			"total_amount": "sum",
			"trip_distance": "sum",
			"passenger_count": "sum"
		}).rename({"vendor_id": "num_trips"}).to_frame().T # type: ignore

		aggregate["period"] = current_date

		result = pd.concat([result, aggregate])

		current_date += timedelta(days=7)

	# clean up the formatting of the dataframe
	result['num_trips'] = result['num_trips'].astype(int)
	result['passenger_count'] = result['passenger_count'].astype(int)
	result['total_amount'] = result['total_amount'].round(2).astype(float)
	result['trip_distance'] = result['trip_distance'].round(2).astype(float)
	result = result[["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]]
	result = result.sort_values(by="period")

	result.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
