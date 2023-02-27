import os
from google.cloud import storage
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./credentials.json"
bucket_name = "dtc_data_lake_dtc-de-376416"
client = storage.Client()
bucket = client.bucket(bucket_name)

months = range(1, 13)
base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
filename_template = "fhv_tripdata_2019-{:02d}.csv.gz"

for month in months:
    # Construindo a URL para download do arquivo
    filename = filename_template.format(month)
    url = base_url + filename

    # Baixando usando wget
    os.system(f"wget {url}")
    os.system(f"gunzip {filename}")

    # removendo extensão do filename
    filename_no_ext = filename.split(".")[0]  # fhv_tripdata_2019-'''

    # inserindo extensão csv e atribuindo à variavel
    csv_filename = f"{filename_no_ext}.csv"

    print(csv_filename)  # fhv_tripdata_2019-'''.csv

    with open(csv_filename) as f:
        df = pd.read_csv(f)

    print(df)

    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)

    # convertendo para parquet
    table = pa.Table.from_pandas(df)
    parquet_filename = f"{filename_no_ext}.parquet"
    pq.write_table(table, parquet_filename)

    # upload no gcs bucket
    blob = bucket.blob("data/fhv-ny_taxi_parquet/" + parquet_filename)
    blob.upload_from_filename(parquet_filename)


# TODO: writing directly to the Google Cloud Storage with gcsfs
# TODO: error handling for downloaded/written/uploaded
# TODO: logging statements
# TODO: no hardcoding credentials
# TODO: wrap to a main function
