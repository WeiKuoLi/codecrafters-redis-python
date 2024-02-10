# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen(5)

    while True:
        # wait for client
        client_socket, client_address = server_socket.accept()        
        with client_socket:
            print(f"connected to  {client_address}")
            while True:
                # get ping
                received_message = client_socket.recv(1024).decode()
                if not received_message:
                    break
                print(f"Received {received_message}")
                
                response_message = "+PONG\r\n"
                client_socket.send(response_message.encode())    
if __name__ == "__main__":
    main()
