import argparse
import clipboard
import errno
import hashlib
import socket
import sys
import threading
import os
from time import sleep
from tqdm import tqdm
from config import MAX_THREADS_COUNT, MSG_TYPE_SIZE, CHUNK_SIZE, SERVER_PORT, SERVER_ADDRESS

peers_semaphore = threading.BoundedSemaphore(MAX_THREADS_COUNT)

def print_help():
    print("""
    usage: python peer.py id <comand> [<args> ...]

    A simple peer application for my p2p file sharing implementation.

    args:
        id                User identification
        file_path         Path to the file to be uploaded
        file_name         Name of the file to be downloaded
        port_number       Number of the port to start seeding

    commands:
        donwload <file_name>                     Donwload a file 
        upload <file_path>                       Upload a file
        seed [port_number]                       Start seeding
        list                                     List available files to download
""")

def parse_args() -> (str, list[str]):
    args = sys.argv[1:]
    if len(args) == 0:
        print("User identification expected.")
        print_help()
        sys.exit(1)
    if len(args) == 1:
        print("Command expected.")
        print_help()
        sys.exit(1)

    if args[1] in ["upload", "download"] and len(args) != 3:
        print(f"Expected 1 argument(s) but found {len(args) - 2}.")
        print_help()
        sys.exit(1)

    if args[1] in ["seed"] and len(args) != 1 and len(args) != 2:
        print(f"Expected at least 0 argument(s) and at most 1 argument(s) but found {len(args) - 2}.")
        print_help()
        sys.exit(1)

    if args[1] in ["list"] and len(args) != 2:
        print(f"Expected 0 argument(s) but found {len(args) - 2}.")
        print_help()
        sys.exit(1)

    user_identification = args[0]

    match args[1]:
        case "download":
            file_name = args[2]
            return (user_identification, "download", [file_name])
        case "upload":
            file_path = args[2]
            if not os.path.exists(file_path):
                print(f"Invalid path to file: '{file_path}'")
                print_help()
                sys.exit(1)
            return (user_identification, "upload", [file_path])
        case "list":
            return (user_identification, "list", [])
        case "seed":
            if len(args) <= 3:
                return (user_identification, "seed", [0])

            port = args[3]
            if not port.isnumeric():
                print(f"Invalid port.")
                print_help()
                sys.exit(1)
            port = int(port)

            if not is_port_available(int(port)):
                print(f"Port already in use.")
                print_help()
                sys.exit(1)

            return (user_identification, "seed", [port])
        case _:
            print(f"Invalid '{args[1]}' command.")
            print_help()
            sys.exit(1)

def is_port_available(port_number: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(('', port_number))
        return True
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            return False
        else:
            raise e
    finally:
        s.close()

def choose_peer(peers: list[(str, int)]) -> (str, int):
    active_peers: dict[(str,int), int] = dict()
    for peer in peers:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect(peer)
            s.sendall(f"PPQ_PPING".encode())
            data = s.recv(MSG_TYPE_SIZE + CHUNK_SIZE)
            if not data and data[MSG_TYPE_SIZE:] != b"PPS_PPONG":
                raise ConnectionError
            active_peers[peer] = data.decode()[MSG_TYPE_SIZE:]
        except (ConnectionError, TimeoutError):
            continue
        finally:
            s.settimeout(None)
            s.close()
    return min(active_peers, key=lambda k: active_peers[k], default=None)

def chunk_file(file_path: str) -> list[bytes]:
    chunks: list[bytes] = []

    with open(file_path, "rb") as f:
        while True:
            chunk: bytes = f.read(CHUNK_SIZE)
            if not chunk:
                break

            chunks.append(chunk)

    return chunks

def hash_chunks(chunks: list[bytes]) -> list[str]:
    chunk_hashes: list[str] = []

    for chunk in chunks:
        chunk_hash = hashlib.sha1(chunk).hexdigest()
        chunk_hashes.append(chunk_hash)

    return chunk_hashes

def save_chunks(chunks: list[bytes], chunk_hashes: list[str], chunks_dir: str):
    if not os.path.exists(chunks_dir):
        os.mkdir(chunks_dir)

    for chunk, chunk_hash in zip(chunks, chunk_hashes):
        with open(f"{chunks_dir}/{chunk_hash}", "wb") as f:
            f.write(chunk)
        chunks.append(chunk_hash)

def send_ppq_msg(s: socket.socket, msg: str) -> bytes:
    s.sendall(msg.encode())
    data = s.recv(MSG_TYPE_SIZE + CHUNK_SIZE)
    s.close()

    if data[:MSG_TYPE_SIZE] == b"PPS_ERROR ":
        print(f"Error from server: {data[MSG_TYPE_SIZE:]}")
        sys.exit(5)

    return data[MSG_TYPE_SIZE:]

def send_csq_msg(msg: str) -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_ADDRESS, SERVER_PORT))
    except ConnectionRefusedError:
        print("Could not connect to server - make sure it's running")
        sys.exit(1)
    except Exception as e:
        print(f"Connection error: {e}")
        sys.exit(1)

    s.send(msg.encode())
    data = s.recv(MSG_TYPE_SIZE + CHUNK_SIZE)
    s.close()

    if data[:MSG_TYPE_SIZE] == b"CSS_ERROR ":
        print(f"Error from server: {data[MSG_TYPE_SIZE:]}")
        sys.exit(5)

    return data[MSG_TYPE_SIZE:].decode()

def get_files() -> list[str]:
    # Request all files tracked by the server along with their hashes
    data = send_csq_msg("CSQ_GETAF")
    files = data.split()
    return dict(zip(files[::2], files[1::2]))

def announce_chunks(user_identification: str, addr: str, port: int):
    chunks_dir = f"chunks/{user_identification}"
    if not os.path.exists(chunks_dir):
        print("You dont have any chunks to seed. Try uploading a file.")
        print_help()
        sys.exit(0)
    chunk_hashes = " ".join(os.listdir(chunks_dir))
    send_csq_msg(f"CSQ_REGCK {addr}:{port} {chunk_hashes}")

def deannounce_peers(peers: str):
    send_csq_msg(f"CSQ_URGPR {peers}")

def upload_file(user_identification: str, file_path: str):
    # Split file into chunks
    chunks = chunk_file(file_path)
    chunk_hashes = hash_chunks(chunks)

    # Register the file within the server
    _, file_name = os.path.split(file_path)
    ok = send_csq_msg(f"CSQ_REGFI {file_name} {' '.join(chunk_hashes)}")

    chunks_dir = f"chunks/{user_identification}"
    if not os.path.exists(chunks_dir):
        os.mkdir(chunks_dir)
    save_chunks(chunks, chunk_hashes, chunks_dir) # TODO! - Retransmit before saving if ok(data) number != len(chunks)

    if int(ok) != len(chunk_hashes):
        print("Failed uploading file. Upload incomplete.")
        sys.exit(1)

    print("Upload completed.")
    
def download_file(user_identification: str, file_name: str):
    # Request chunk_hashes for the file (queried by file_hash)

    data = send_csq_msg(f"CSQ_GETFI {file_name}")
    chunk_hashes = data.split()

    file_chunks: dict[str, bytes] = { chunk_hash: None for chunk_hash in chunk_hashes }
    semaphore = threading.BoundedSemaphore(MAX_THREADS_COUNT)
    chunk_threads = []

    def handle_chunk(user_identification: str, chunk_hash: str, peer: (str, int)):
        semaphore.acquire()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(peer)

        data = send_ppq_msg(s, f"PPQ_GETCK {chunk_hash}")

        if chunk_hash in file_chunks.keys():
            file_chunks[chunk_hash] = data
            if not os.path.exists(f"chunks/{user_identification}"):
                os.mkdir(f"chunks/{user_identification}")
            with open(f"chunks/{user_identification}/{chunk_hash}", "wb") as f:
                f.write(data)
        semaphore.release()

    if not chunk_hashes:
        print("File not available.")
        sys.exit(0)

    # Request peers for each chunk
    for chunk_hash in tqdm(chunk_hashes):
        data = send_csq_msg(f"CSQ_GETCK {chunk_hash}")

        if not data:
            print("No active peers seeding.")
            sys.exit(0)

        peers = [(peer.split(":")[0], int(peer.split(":")[1])) for peer in data.split()]
        peer = choose_peer(peers)
        if peer is None:
            deannounce_peers(" ".join(data))
            print("No active peers seeding.")
            sys.exit(0)

        chunk_thread = threading.Thread(
            target=handle_chunk,
            args=(user_identification, chunk_hash, peer)
        )
        chunk_thread.start()
        chunk_threads.append(chunk_thread)
        
        # sleep(0.01)

    for thread in chunk_threads:
        thread.join()
    
    if None in file_chunks.values():
        print("Missing some chunks, download incomplete")
        sys.exit(1)

    downloads_dir = f"downloads/{user_identification}"
    if not os.path.exists(downloads_dir):
        os.mkdir(downloads_dir)
    for chunk_hash, chunk in file_chunks.items():

        if chunk_hash != hashlib.sha1(chunk).hexdigest():
            print("Integrity check failed. Download compromised.")
            sys.exit(1)

        with open(f"{downloads_dir}/{file_name}", "ab") as f:
            f.write(chunk)

def handle_peer(user_identification: str, peer_socket: socket.socket, peer_addr: (str, int)):
    peers_semaphore.acquire()
    data = peer_socket.recv(MSG_TYPE_SIZE + CHUNK_SIZE).decode()
    if not data:
       return 
    print(f"Received message: {data}")
    req_type = data[:MSG_TYPE_SIZE-1]
    match req_type:

        case "PPQ_PPING":
            msg = f"PPS_PPONG {threading.active_count() - 1}"
            peer_socket.sendall(msg.encode())
            print(f"Message sent: {msg}")

        case "PPQ_GETCK":
            chunk_hash = data[MSG_TYPE_SIZE:]

            chunk_dir = f"chunks/{user_identification}/{chunk_hash}"
            if not os.path.exists(chunk_dir):
                msg = "PPS_ERROR Chunk unavailable on requested peer."
                peer_socket.sendall(msg.encode())
                peer_socket.close()
                print("You dont hold the requested chunk [TODO].")
                print_help()
                sys.exit(0)

            with open(chunk_dir, "rb") as f:
                chunk = f.read()
            msg = b"PPS_GETCK " + chunk
            peer_socket.send(msg)
            print(f"Message sent. [data from {chunk_hash}]")

        case _:
            msg = f"CSS_ERROR Unknown request type '{req_type}'."
            peer_socket.sendall(msg.encode())
            print(f"Message sent: {msg}")
            peer_socket.close()

    peer_socket.close()
    peers_semaphore.release()
    
if __name__ == "__main__":

    user_identification, cmd, cmd_args = parse_args()

    match cmd:

        case "download":
            [file_name] = cmd_args
            if not os.path.exists("downloads"):
                os.mkdir("downloads")
            download_file(user_identification, file_name)

        case "upload":
            [file_path] = cmd_args
            if not os.path.exists("chunks"):
                os.mkdir("chunks")
            upload_file(user_identification, file_path)

        case "list":
            files = get_files()
            if not files:
                print("No files available.")
                sys.exit(0)
            while True:
                for i, (file_name, n_seeds) in enumerate(files.items()):
                    print(f"({i}) {file_name} {n_seeds}")
                print("\n**[n]number of seeds for the file")

                try:
                    file_i = input("Type file number (n) to copy name to clipboard: ")
                except KeyboardInterrupt:
                    print("\nClosing...")
                    sys.exit(0)

                if not file_i.isnumeric() or int(file_i) > len(files) - 1:
                    print("Invalid number.")
                    continue
                file = list(files.keys())[int(file_i)]
                clipboard.copy(file) 
                print(f"Copied file name: {file}")
                break

        case "seed":
            [port_number] = cmd_args
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('', port_number))
            s.listen()
            print(f"Seeding on {s.getsockname()[0]}:{s.getsockname()[1]}.")
            announce_chunks(user_identification, s.getsockname()[0], s.getsockname()[1])
            while True:
                try:
                    if threading.active_count() >= MAX_THREADS_COUNT:
                        continue
                    peer_socket, peer_addr = s.accept()
                    print(f"New connection from {peer_addr}")
                    peer_thread = threading.Thread(
                        target=handle_peer,
                        args=(user_identification, peer_socket, peer_addr),
                        daemon=True
                    )
                    peer_thread.start()
                except KeyboardInterrupt:
                    print("Closing...")
                    break
                except Exception as e:
                    print(f"Conection error: {e}")
                    break

            deannounce_peers(f"{s.getsockname()[0]}:{s.getsockname()[1]}")
            s.close()
            sys.exit(0)

        case _:
            print(f"Invalid '{args[0]}' command.")
            print_help()
            sys.exit(0)
