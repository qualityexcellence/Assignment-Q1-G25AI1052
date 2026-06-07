import json
import socket
import sys


NODES = {
    "1": ("localhost", 5001),
    "2": ("localhost", 5002),
    "3": ("localhost", 5003),
    "4": ("localhost", 5004),
    "5": ("localhost", 5005)
}


def send_fake_prepare(target_node):

    host, port = NODES[target_node]

    #
    # Fake PBFT_PREPARE
    # Invalid signature
    #

    malicious_message = {
        "type": "PBFT_PREPARE",
        "sender": "999",
        "sequence": 1,
        "payload": {
            "txid": "EVIL_TX",
            "from": "Mallory",
            "to": "Attacker",
            "amount": 999999
        },
        "signature": "FAKE_SIGNATURE"
    }

    try:

        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        sock.connect(
            (host, port)
        )

        sock.send(
            json.dumps(
                malicious_message
            ).encode()
        )

        sock.close()

        print(
            f"Malicious PREPARE sent to Node {target_node}"
        )

    except Exception as e:

        print(
            f"Failed: {e}"
        )


def send_fake_commit(target_node):

    host, port = NODES[target_node]

    malicious_message = {
        "type": "COMMIT",
        "sender": "999",
        "sequence": 1,
        "payload": {
            "txid": "EVIL_COMMIT",
            "from": "Mallory",
            "to": "Attacker",
            "amount": 999999
        },
        "signature": "FAKE_SIGNATURE"
    }

    try:

        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        sock.connect(
            (host, port)
        )

        sock.send(
            json.dumps(
                malicious_message
            ).encode()
        )

        sock.close()

        print(
            f"Malicious COMMIT sent to Node {target_node}"
        )

    except Exception as e:

        print(
            f"Failed: {e}"
        )


if __name__ == "__main__":

    if len(sys.argv) != 3:

        print(
            "Usage:"
        )

        print(
            "python adversary.py "
            "<prepare|commit> "
            "<target_node>"
        )

        sys.exit(1)

    attack_type = sys.argv[1]

    target_node = sys.argv[2]

    if attack_type == "prepare":

        send_fake_prepare(
            target_node
        )

    elif attack_type == "commit":

        send_fake_commit(
            target_node
        )

    else:

        print(
            "Unknown attack type"
        )