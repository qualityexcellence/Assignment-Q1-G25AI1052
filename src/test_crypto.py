from crypto_utils import CryptoUtils, PBFTMessage

priv, pub = CryptoUtils.generate_key_pair("node1")

private_key = CryptoUtils.load_private_key(priv)
public_key = CryptoUtils.load_public_key(pub)

payload = {
    "txid": "TX1001",
    "from": "Alice",
    "to": "Bob",
    "amount": 1000
}

msg = PBFTMessage.create_message(
    "PRE_PREPARE",
    "node1",
    1,
    payload
)

signed = PBFTMessage.sign(
    private_key,
    msg
)

print("Signed")

result = PBFTMessage.verify(
    public_key,
    signed
)

print("Valid:", result)
