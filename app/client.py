import socket

# Define the server's IP address and port number
server_ip = "localhost"  # Replace with the server's actual IP address
server_port = 6379      # Replace with the server's actual port number

# Create a socket object for the client
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

print("clien socket initiated")

# Connect the client socket to the server
client_socket.connect((server_ip, server_port))

# Send data to the server
message = "*1\r\n$4\r\nping\r\n"
try:
    client_socket.send(message.encode())
except:
    print("message sent failed")

# Receive data from the server
data = client_socket.recv(1024)
print("Received data from server:", data.decode())

# Close the connection
client_socket.close()

