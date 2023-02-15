# Sherpas Data Engineer Take Home

## Summary

The main deliverable of this project is a pipeline that does the following:
1. Reads an encrypted byte string file from GCS that contains user data
2. Decrypts the data using a private RSA pem key
3. Writes the decrypted data to GCS as a JSON file
4. Writes the decrypted data to BigQuery

## Prerequisites

1. Install Python dependencies:
```
pip install -r requirements.txt
```

2. Provision a cloud storage bucket in your project called `ta_data_engineer`. This could have been codified using terraform, but that step was skipped for the sake of time.

3. Provision a BigQuery dataset in your project called `user_data`. This could have been codified using terraform, but that step was skipped for the sake of time.

4. Run 
```
python setup.py
``` 

This will accomplish the following:
- generate a private and a public RSA key for encrypting and decrypting data. These will be persisted to the `utils` directory for later use.
- encrypt each file in the `data` directory and save those files to `gs://ta_data_engineer/encrypted_files`

## Running the pipeline

The pipeline (summarized above) can be run to process all encrypted files in `gs://ta_data_engineer/encrypted_files` by running:
```
python main.py
```

This will result in decrypted data being saved to `gs://ta_data_engineer/decrypted_files` and a BigQuery dataset called `user_data.users`. For the sake of time, the insertion of duplicate records to BigQuery was not accounted for. 

## Airflow Stretch Goal

The pipeline was wrapped in an Airflow DAG in `main.py` which was successfully tested by running 
```
airflow tasks test sherpas_ta decrypt_and_write_pipeline
```
 This assumes that the `dags_folder` is set to `/Users/anthonyabercrombie/sherpas_ta_de` in your `airflow.cfg`.
