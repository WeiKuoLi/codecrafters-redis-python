import socket
import argparse
from app.src.redisdata import RedisObject
import pdb

def client_session(server_ip="localhost", server_port=6379):

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
            try:
                _inp = input()
            except:
                break
            
            _inp_list = _inp.split(' ')
            _inp_list = [RedisObject(obj=_, typ="bulk_str") for _ in _inp_list]
            _inp_redis_obj= RedisObject(obj=_inp_list, typ="list")
            
            if(_inp != "\n"):
                message = str(_inp_redis_obj)
            #message = "*3\r\n$4\r\nping\r\n$4\r\necho\r\n$4\r\npool\r\n"
            try:
                client_socket.send(message.encode())
                #print(f"Message: {message.__repr__()} sent")
            except:
                print("Message sent fail")
            # Receive data from the server
            data = client_socket.recv(1024)
            #print(f"data is {data.decode()}")
            #print("Received data from server:",
            print(RedisObject.from_string(data.decode()).__repr__())

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


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="redis server port number(default to 6379)")

    args = parser.parse_args()
    
    if(args.port is None):
        port_number = 6379
    else:
        port_number = int(args.port)
    
    client_session(server_port=port_number)
