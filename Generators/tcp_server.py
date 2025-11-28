# tcp_server.py
import socket

HOST = "localhost"
PORT = 5050

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen()
        print(f"Server listening on {HOST}:{PORT}")

        conn, addr = server.accept()
        print(f"Connected by {addr}")

        buffer = ""

        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    print("Client disconnected.")
                    break

                buffer += data.decode()

                # Process newline-delimited messages from client
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    print("Received:", line)

if __name__ == "__main__":
    main()
