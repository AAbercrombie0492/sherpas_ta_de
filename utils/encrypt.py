import json
import rsa
from pathlib import Path
from typing import Tuple, Dict

def generate_rsa_keys(pem_key_dir: str) -> Tuple[rsa.key.PublicKey, rsa.key.PrivateKey]:
    """
    Generate a pair of RSA keys and save them to the given directory as PEM files.

    Args:
        pem_key_dir (str): Path to the directory where the PEM files will be saved.

    Returns:
        Tuple[rsa.key.PublicKey, rsa.key.PrivateKey]: The generated RSA keys.
    """
    public_key_path = Path(pem_key_dir) / "public_key.pem"
    private_key_path = Path(pem_key_dir) / "private_key.pem"

    if not (public_key_path.exists() and private_key_path.exists()):
        public_key, private_key = rsa.newkeys(2048)
        with open(public_key_path, "wb") as f:
            f.write(public_key.save_pkcs1())
        with open(private_key_path, "wb") as f:
            f.write(private_key.save_pkcs1())
    else:
        with open(public_key_path, "rb") as f:
            public_key = rsa.PublicKey.load_pkcs1(f.read())
        with open(private_key_path, "rb") as f:
            private_key = rsa.PrivateKey.load_pkcs1(f.read())

    return public_key, private_key

def encrypt_json_and_save_file(input_fpath: str, output_fpath: str, public_key: rsa.key.PublicKey) -> None:
    """
    Encrypt a JSON file using the given public key and save the encrypted data to the output file.

    Args:
        input_fpath (str): Path to the input JSON file.
        output_fpath (str): Path to the output file where the encrypted data will be saved.
        public_key (rsa.key.PublicKey): Public key used to encrypt the data.

    Returns:
        None
    """
    with open(input_fpath, "r") as f:
        data = json.load(f) 

    message = json.dumps(data)
    enc_message = rsa.encrypt(message.encode(),
                            public_key)

    with open(output_fpath, "wb") as f:
        f.write(enc_message)

def decrypt_file(input_path: str, private_key: rsa.key.PrivateKey) -> Dict:
    """
    Decrypt an encrypted file using the given private key.

    Args:
        input_path (str): Path to the input encrypted file.
        private_key (rsa.key.PrivateKey): Private key used to decrypt the data.

    Returns:
        Dict: Decrypted data as a dictionary.
    """
    with open(input_path, "rb") as f:
        enc_message = f.read()

    return decrypt_message(enc_message, private_key)

def decrypt_message(enc_message: bytes, private_key: rsa.key.PrivateKey) -> Dict:
    """
    Decrypt a message using the given private key.

    Args:
        enc_message (bytes): Encrypted message as bytes.
        private_key (rsa.key.PrivateKey): Private key used to decrypt the message.

    Returns:
        Dict: Decrypted message as a dictionary.
    """
    dec_message = rsa.decrypt(enc_message, private_key).decode()
    return json.loads(dec_message)

def integration_test():
    """
    Encrypt 3 files from the data directory and then decrypt them for validation
    """
    public_key, private_key = generate_rsa_keys('.')

    encrypt_json_and_save_file("../data/user_a.json", "../data/user_a", public_key)
    encrypt_json_and_save_file("../data/user_b.json", "../data/user_b", public_key)
    encrypt_json_and_save_file("../data/user_c.json", "../data/user_c", public_key)

    decrypted_a= decrypt_file("../data/user_a", private_key)
    print(decrypted_a)
    decrypted_b= decrypt_file("../data/user_b", private_key)
    print(decrypted_b)
    decrypted_c= decrypt_file("../data/user_c", private_key)
    print(decrypted_c)

if __name__ == "__main__":
    integration_test()