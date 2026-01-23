# import necessary libraries
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click

...


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')


def run_ingest(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    # set up the database connection
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Format month with zero-padding
    month_str = f'{month:02d}'
    prefix ='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month_str}.csv.gz'

    # define the data types for each column
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

    # create an iterator to read the CSV file in chunks
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
    chunksize = chunksize
)

    # ingest the data chunk by chunk
    first_chunk = True
    for df_chunk in tqdm(df_iter):
        if first_chunk:
            df_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
        first_chunk = False
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

if __name__ == '__main__':
    run_ingest()