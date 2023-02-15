from utils import io, encrypt
import json
import rsa
from typing import List
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def pipeline(fname: str, bucket_name: str, encrypted_subbucket: str, decrypted_subbucket: str, private_key: rsa.key.PrivateKey):
    """
    Reads encrypted data from Google Cloud Storage (GCS), decrypts it, saves the decrypted data back to GCS and writes
    it to BigQuery.

    Args:
        fname (str): The name of the file to decrypt.
        bucket_name (str): The name of the bucket where all data is to be stored.
        encrypted_subbucket (str): The subbucket where the encrypted file is stored.
        decrypted_subbucket (str): The subbucket where the decrypted file will be stored.
        private_key (rsa.key.PrivateKey): Private key used to decrypt the message.

    Returns:
        None
    """    
    # Read encrypted data from GCS
    encrypted_byte_string = io.read_byte_string_from_gcs(bucket_name, f"{encrypted_subbucket}/{fname}")

    # Decrypt the data
    decrypted_dict = encrypt.decrypt_message(encrypted_byte_string, private_key)

    # Write the decrypted data to GCS
    io.save_byte_string_to_gcs(bucket_name, f"{decrypted_subbucket}/{fname}.json", json.dumps(decrypted_dict).encode('utf-8'), content_type='application/json')

    # Write the decrypted data to BigQuery
    io.write_dict_to_bigquery("user_data", "users", decrypted_dict)

def main():
    public_key, private_key = encrypt.generate_rsa_keys('./utils')

    bucket_name = "ta_data_engineer"
    encrypted_subbucket = "encrypted_files"
    decrypted_subbucket = "decrypted_files"
    files = [
        "user_a",
        "user_b",
        "user_c"
        ]

    for fname in files:
        pipeline(
            fname=fname, 
            bucket_name="ta_data_engineer", 
            encrypted_subbucket="encrypted_files",
            decrypted_subbucket="decrypted_files",
            private_key=private_key
        )

if __name__=="__main__":
    main()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sherpas_ta', default_args=default_args, schedule_interval=None)

public_key, private_key = encrypt.generate_rsa_keys('./utils')

task_pipeline = PythonOperator(
    task_id='decrypt_and_write_pipeline',
    python_callable=pipeline,
    op_kwargs={
        'fname': 'user_a',
        'bucket_name': 'ta_data_engineer',
        'encrypted_subbucket': 'encrypted_files',
        'decrypted_subbucket': 'decrypted_files',
        'private_key': private_key
    },
    dag=dag
)