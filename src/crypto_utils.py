import json
import base64
import hashlib
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization


KEY_DIR = Path("keys")
KEY_DIR.mkdir(exist_ok=True)


class CryptoUtils:

    @staticmethod
    def generate_key_pair(node_id: str):

        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )

        public_key = private_key.public_key()

        private_path = KEY_DIR / f"{node_id}_private.pem"
        public_path = KEY_DIR / f"{node_id}_public.pem"

        with open(private_path, "wb") as f:
            f.write(
                private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            )

        with open(public_path, "wb") as f:
            f.write(
                public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                )
            )

        return str(private_path), str(public_path)

    @staticmethod
    def load_private_key(path: str):

        with open(path, "rb") as f:
            return serialization.load_pem_private_key(
                f.read(),
                password=None
            )

    @staticmethod
    def load_public_key(path: str):

        with open(path, "rb") as f:
            return serialization.load_pem_public_key(
                f.read()
            )

    @staticmethod
    def hash_message(message):

        if isinstance(message, dict):
            message = json.dumps(
                message,
                sort_keys=True
            )

        if isinstance(message, str):
            message = message.encode()

        return hashlib.sha256(message).hexdigest()

    @staticmethod
    def sign_message(private_key, message):

        if isinstance(message, dict):
            message = json.dumps(
                message,
                sort_keys=True
            )

        if isinstance(message, str):
            message = message.encode()

        signature = private_key.sign(
            message,
            padding.PKCS1v15(),
            hashes.SHA256()
        )

        return base64.b64encode(signature).decode()

    @staticmethod
    def verify_signature(public_key, message, signature):

        try:

            if isinstance(message, dict):
                message = json.dumps(
                    message,
                    sort_keys=True
                )

            if isinstance(message, str):
                message = message.encode()

            public_key.verify(
                base64.b64decode(signature),
                message,
                padding.PKCS1v15(),
                hashes.SHA256()
            )

            return True

        except Exception:
            return False


class PBFTMessage:

    @staticmethod
    def create_message(
        msg_type,
        sender,
        sequence,
        payload
    ):

        return {
            "type": msg_type,
            "sender": sender,
            "sequence": sequence,
            "payload": payload,
            "digest": CryptoUtils.hash_message(payload)
        }

    @staticmethod
    def sign(private_key, message):

        signature = CryptoUtils.sign_message(
            private_key,
            message
        )

        msg = dict(message)
        msg["signature"] = signature

        return msg

    @staticmethod
    def verify(public_key, message):

        if "signature" not in message:
            return False

        signature = message["signature"]

        copy_msg = dict(message)
        del copy_msg["signature"]

        return CryptoUtils.verify_signature(
            public_key,
            copy_msg,
            signature
        )
