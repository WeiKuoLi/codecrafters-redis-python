import socket

# Define the server's IP address and port number
server_ip = "localhost"  # Replace with the server's actual IP address
server_port = 6379      # Replace with the server's actual port number

# Create a socket object for the client
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

print("Client socket initiated")
try:
    # Connect the client socket to the server
    client_socket.connect((server_ip, server_port))
    print("Connected to server")
    _inp = "+ping\r\n"
    while True:
        # Send data to the server
        #message = "*4\r\n$4\r\nping\r\n$4\r\necho\r\n$3\r\vlol\r\v$4\r\nping\r\n"
        try:
            _inp = input()
        except:
            break

        if(_inp != "\n"):
            message = "\r\n".join(_inp.split(" "))
        #message = "*3\r\n$4\r\nping\r\n$4\r\necho\r\n$4\r\npool\r\n"
        try:
            client_socket.send(message.encode())
            print(f"Message: {message} sent")
        except:
            print("Message sent fail")
        # Receive data from the server
        data = client_socket.recv(1024)
        #print(f"data is {data.decode()}")
        print("Received data from server:", data.decode())

except ConnectionRefusedError:
    print("Connection refused: Server is not running or unreachable")
except KeyboardInterrupt:
    print("endding client connection")
except Exception as e:
    print("An error occurred:", e)

finally:
    # Close the connection
    client_socket.close()
    print("Client socket closed")

