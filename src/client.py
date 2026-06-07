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


def send_transaction(
    node_id,
    txid,
    sender,
    receiver,
    amount
):

    host, port = NODES[node_id]

    transaction = {
        "txid": txid,
        "from": sender,
        "to": receiver,
        "amount": amount
    }

    message = {
        "type": "CLIENT_TX",
        "transaction": transaction
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
            json.dumps(message).encode()
        )

        sock.close()

        print(
            f"Transaction sent to Node {node_id}"
        )

        print(
            json.dumps(
                transaction,
                indent=4
            )
        )

    except Exception as e:

        print(
            f"Failed: {e}"
        )


if __name__ == "__main__":

    if len(sys.argv) != 6:

        print(
            "Usage:"
        )

        print(
            "python client.py "
            "<leader_node_id> "
            "<txid> "
            "<from> "
            "<to> "
            "<amount>"
        )

        sys.exit(1)

    node_id = sys.argv[1]

    txid = sys.argv[2]

    sender = sys.argv[3]

    receiver = sys.argv[4]

    amount = float(
        sys.argv[5]
    )

    send_transaction(
        node_id,
        txid,
        sender,
        receiver,
        amount
    )