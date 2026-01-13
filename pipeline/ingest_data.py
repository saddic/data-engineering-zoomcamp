# import necessary libraries
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine






def run_ingest():
    # set up the database connection
    pg_user = 'root'
    pg_password = 'root'
    pg_host = 'localhost'
    pg_port = '5432'
    pg_db = 'ny_taxi'
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    # define the URL of the dataset
    month = '01'
    year = '2021'
    target_table = 'yellow_taxi_data'
    chunksize = 100000
    prefix ='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month}.csv.gz'

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