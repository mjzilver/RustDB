import socket
import argparse

HOST = "127.0.0.1"
PORT = 4210
COUNT = 10_000
BATCH = 1000


def send_batch(sock, file, cmds):
    sock.sendall(("\n".join(cmds) + "\n").encode())
    return [file.readline().rstrip("\n") for _ in cmds]


def put_keys(sock, file):
    for i in range(0, COUNT, BATCH):
        cmds = [f"put key{j} value{j}" for j in range(i, i + BATCH)]
        resps = send_batch(sock, file, cmds)
        for r in resps:
            if r.startswith("ERR"):
                raise RuntimeError(f"put failed: {r}")
    print("PUT complete")


def get_keys(sock, file):
    for i in range(0, COUNT, BATCH):
        cmds = [f"get key{j}" for j in range(i, i + BATCH)]
        resps = send_batch(sock, file, cmds)
        for j, r in enumerate(resps, start=i):
            if r != f"value{j}":
                raise RuntimeError(f"get failed for key{j}: {r}")
    print("GET complete")


def delete_keys(sock, file):
    for i in range(0, COUNT, BATCH):
        cmds = [f"delete key{j}" for j in range(i, i + BATCH)]
        resps = send_batch(sock, file, cmds)
        for r in resps:
            if r.startswith("ERR"):
                raise RuntimeError(f"delete failed: {r}")
    print("DELETE complete")


def verify_deleted(sock, file):
    for i in range(0, COUNT, BATCH):
        cmds = [f"get key{j}" for j in range(i, i + BATCH)]
        resps = send_batch(sock, file, cmds)
        for j, r in enumerate(resps, start=i):
            if not r.startswith("ERR"):
                raise RuntimeError(f"key still exists: key{j} -> {r}")
    print("VERIFY DELETE complete")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--put", action="store_true")
    parser.add_argument("--get", action="store_true")
    parser.add_argument("--delete", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    do_put = args.put or args.full
    do_get = args.get or args.full
    do_delete = args.delete or args.full

    if not (do_put or do_get or do_delete):
        parser.error("No operation specified. Use --put, --get, --delete, or --full.")

    sock = socket.create_connection((HOST, PORT))
    file = sock.makefile("r")

    if do_put:
        put_keys(sock, file)

    if do_get:
        get_keys(sock, file)

    if do_delete:
        delete_keys(sock, file)

    if do_delete:
        verify_deleted(sock, file)

    print("ALL SELECTED TESTS PASSED")
    sock.close()


if __name__ == "__main__":
    main()
