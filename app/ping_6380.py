import socket

# Define the server's IP address and port number
server_ip = "localhost"  # Replace with the server's actual IP address
server_port = 6380       # Replace with the server's actual port number

# Create a socket object for the client
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

print("Client socket initiated")
try:
    # Connect the client socket to the server
    client_socket.connect((server_ip, server_port))
    print("Connected to server")
    
    for i in range(4):
        # Define the message to send to the server
        message = "$4\r\nping\r\n"  # Adjust this according to the server's protocol
        try:
            # Send data to the server
            client_socket.send(message.encode())
            print(f"Message: {message} sent")
        except Exception as e:
            print("Message sent fail:", e)
        
        # Receive data from the server
        data = client_socket.recv(1024)
        print("Received data from server:", data.decode())

except ConnectionRefusedError:
    print("Connection refused: Server is not running or unreachable")
except Exception as e:
    print("An error occurred:", e)

finally:
    # Close the connection
    client_socket.close()
    print("Client socket closed")

