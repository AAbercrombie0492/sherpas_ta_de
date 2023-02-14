from google.cloud import storage, bigquery
import json
from typing import Optional

def save_byte_string_to_gcs(bucket_name: str, file_name: str, data_byte_string: bytes, content_type:Optional[str]):
    """Saves a JSON file to Google Cloud Storage.

    Args:
        bucket_name (str): The name of the bucket to save the file to.
        file_name (str): The name to give the saved file.
        data (bytes): data to save
        content_type (str) e.g. application/json

    Returns:
        str: The URI of the saved file in Google Cloud Storage.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data_byte_string, content_type=content_type)

    return f'gs://{bucket_name}/{file_name}'

def read_byte_string_from_gcs(bucket_name, file_name):
    """Reads a text file from Google Cloud Storage.

    Args:
        bucket_name (str): The name of the bucket containing the file.
        file_name (str): The name of the file to read.

    Returns:
        byte_string: The contents of the file.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    byte_string = blob.download_as_bytes()

    return byte_string

def write_dict_to_bigquery(dataset_name, table_name, data):
    """Writes a dictionary to BigQuery.

    Args:
        dataset_name (str): The name of the dataset to write to.
        table_name (str): The name of the table to write to.
        data (dict): A dictionary to write to the table.

    Returns:
        None
    """
    client = bigquery.Client()
    table_ref = client.dataset(dataset_name).table(table_name)

    # Define the schema of the table
    schema = []
    for key, value in data.items():
        schema.append(bigquery.SchemaField(key, "STRING"))

    # Create the table if it does not already exist
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)

    # Insert the data into the table
    rows_to_insert = [data]
    errors = client.insert_rows(table, rows_to_insert)

    # Check for errors
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print("Successfully inserted row.")


def integration_test():
    public_key, private_key = generate_rsa_keys('')

    bucket_name = "ta_data_engineer"

    fnames = [
        "user_a",
        "user_b",
        "user_c"
    ]
    for fname in fnames:
        # Encrypt original user json and save byte string
        encrypt_json_and_save_file(f"../data/{fname}.json", f"../data/{fname}", public_key)

        # Load encrypted data
        with open(f'../data/{fname}', 'rb') as f:
            user_encrypted_original = f.read()

        # Write encrypted data to GCS
        save_byte_string_to_gcs(bucket_name, f"encrypted_files/{fname}", user_encrypted_original, content_type='text/plain')
        
        # Read encrypted data from GCS
        user_encrypted_loaded = read_byte_string_from_gcs(bucket_name, f"encrypted_files/{fname}")
        assert user_encrypted_loaded == user_encrypted_original

        # Write decrypted data to GCS
        user_decrypted = decrypt_message(user_encrypted_loaded, private_key)
        save_byte_string_to_gcs(bucket_name, f"decrypted_files/{fname}.json", json.dumps(user_decrypted).encode('utf-8'), content_type='application/json')

        # Write decrypted data to BigQuery
        write_dict_to_bigquery("user_data", "users", user_decrypted)

if __name__ == "__main__":
    from encrypt import decrypt_file, decrypt_message, generate_rsa_keys, encrypt_json_and_save_file

    integration_test()