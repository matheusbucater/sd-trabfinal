import json
import socket
import sys
import os
from config import SERVER_ADDRESS, SERVER_PORT, CHUNK_SIZE, MSG_TYPE_SIZE

files: dict[str, list[str]] = dict()
chunks: dict[str, set[str]] = dict()

def get_files():
    msg = ""
    for file_name, chunk_hashes in files.items():
        msg += f"{file_name} "
        if not chunk_hashes:
            msg += "[0] "
            continue
        peer_sets = [set(chunks.get(chunk, [])) for chunk in chunk_hashes]
        common_peers = set.intersection(*peer_sets) if peer_sets else set()
        msg += f"[{len(common_peers)}] "
    return msg[:-1]

if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind((SERVER_ADDRESS, SERVER_PORT))
        s.listen()
        print(f"Listening on {SERVER_ADDRESS}:{SERVER_PORT}")
    except Exception as e:
        print(f"Failed to start server: {e}")
        sys.exit(1)

    while True:
        try:
            client_socket, client_addr = s.accept()
            print(f"New connection from {client_addr}")

            data = client_socket.recv(MSG_TYPE_SIZE + CHUNK_SIZE).decode()
            if not data:
                break
            print(f"Received message: {data}")

            req_type = data[:MSG_TYPE_SIZE-1]
            match req_type:

                case "CSQ_GETAF":
                    msg = f"CSS_GETAF {get_files()}"
                    client_socket.sendall(msg.encode())
                    print(f"Message sent: {msg}")

                case "CSQ_GETFI":
                    file_name = data[MSG_TYPE_SIZE:]
                    msg = f"CSS_GETFI {' '.join(files.get(file_name, ''))}"
                    client_socket.sendall(msg.encode())
                    print(f"Message sent: {msg}")

                case "CSQ_GETCK":
                    chunk_hash = data[MSG_TYPE_SIZE:]
                    msg = f"CSS_GETCK {' '.join(chunks.get(chunk_hash, ''))}"
                    client_socket.sendall(msg.encode())
                    print(f"Message sent: {msg}")

                case "CSQ_REGFI":
                    data = data[MSG_TYPE_SIZE:].split()
                    file_name = data[0]
                    file_chunks = data[1:]
                    files[file_name] = file_chunks
                    for chunk_hash in file_chunks:
                        chunks[chunk_hash] = set()
                    msg = f"CSS_REGFI {len(file_chunks)}"
                    client_socket.sendall(msg.encode())
                    print(f"Message sent: {msg}")

                case "CSQ_REGCK":
                    count = 0
                    data = data[MSG_TYPE_SIZE:].split()
                    peer = data[0]
                    chunk_hashes = data[1:]
                    for chunk in chunk_hashes:
                        if chunk in chunks:
                            chunks[chunk].add(peer)
                            count += 1
                    msg = f"CSS_REGCK {count}"
                    client_socket.sendall(msg.encode())
                    print(f"Message sent: {msg}")

                case "CSQ_URGPR":
                    peers = data[MSG_TYPE_SIZE:].split()
                    for peer in peers:
                        for chunk, chunk_peers in chunks.items():
                            if peer in chunk_peers:
                                chunk_peers.remove(peer)
                    msg = "CSS_URGPR"
                    client_socket.sendall(msg.encode())
                    print(f"Message sent: {msg}")
                        
                case "CSQ_URGCK":
                    count = 0
                    data = data[MSG_TYPE_SIZE:].split()
                    peer = data[0]
                    chunk_hashes = data[1:]
                    for chunk in chunk_hashes:
                        if chunk in chunks:
                            if peer in chunks[chunk]:
                                chunks[chunk].remove(peer)
                                count += 1
                    msg = f"CSS_URGCK {count}"
                    client_socket.sendall(msg.encode())
                    print(f"Message sent: {msg}")

                case _:
                    msg = f"CSS_ERROR Unknown request type '{req_type}'."
                    client_socket.sendall(msg.encode())
                    print(f"Message sent: {msg}")

            print(f"Connection closed with {client_addr}")
        except KeyboardInterrupt:
            print("\nClosing...")
            break
        except Exception as e:
            print(f"Error accepting connection: {e}")
            break
        finally:
            client_socket.close()
    s.close()
