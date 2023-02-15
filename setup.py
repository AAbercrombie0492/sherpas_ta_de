from utils import io, encrypt

public_key, private_key = encrypt.generate_rsa_keys('utils')

bucket_name = "ta_data_engineer"

fnames = [
    "user_a",
    "user_b",
    "user_c"
]
for fname in fnames:
    # Encrypt original user json and save byte string
    encrypt.encrypt_json_and_save_file(f"./data/{fname}.json", f"./data/{fname}", public_key)

    # Load encrypted data
    with open(f'./data/{fname}', 'rb') as f:
        user_encrypted_original = f.read()

    # Write encrypted data to GCS
    io.save_byte_string_to_gcs(bucket_name, f"encrypted_files/{fname}", user_encrypted_original, content_type='text/plain')