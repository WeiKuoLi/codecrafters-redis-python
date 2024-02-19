import cProfile

import argparse
import asyncio
import uuid

from app.src.redisdata import RedisObject
from app.src.redisio import RedisIOHandler
from app.src.redis_slave import RedisServerSlave
from app.src.redis_master import RedisServerMaster

from app.src.rdb import import_rdb_file

async def handle_replica(reader, writer, redis_handler):
    try:
        await redis_handler.redis_server.hand_shake(reader,writer)
        print("handshake with master was successful")
        while True:
           # await asyncio.sleep(2)
            #print('hi')
            received_data = await reader.read(1024)
            if not received_data:
                break
            try:
                received_message = received_data.decode('latin-1')
                print(f"Received {received_message} from master server {address}")
            except:
                received_message = "+ping\r\n"
                print("received message that cannot be decoded")
            input_redisobject = redis_handler.parse_input(received_message)
            #for debug
            print(f"Received from master: {redis_handler.parsed_input.__repr__()}")
            try:
                output_redisobject = redis_handler.execute_master_command(client_id=client_id, input_redisobject=input_redisobject)
            except:
                print("cannot excecute command")
            #for debug
            #print(f"Response master: {redis_handler.parsed_output.__repr__()}")
            
            #response_message = redis_handler.parse_output(output_redisobject)
           # print(f"Response master {response_message} to {address}")
            
            
          #  writer.write(response_message.encode())
          #  await writer.drain()

    except:
        print("app.main.handle_replica error")
        #debug
    finally:
        print(f"Close connection with MASTER")
        writer.close()
        await writer.wait_closed()


async def handle_client(reader, writer, redis_handler):
    client_id = str(uuid.uuid4()) 
    while (client_id in redis_handler.session):
        client_id = str(uuid.uuid4()) 
    
    redis_handler.session[client_id] = { "reader":reader,
                                         "writer": writer,
                                         "client_ip":None,
                                         "client_port":None}
    
    #debug
    #print(f"Connected to {client_id}")
    try:
        while True:
            received_data = await reader.read(1024)
            if not received_data:
                break

            received_message = received_data.decode()
            #print(f"Received {received_message} from {address}")

            input_redisobject = redis_handler.parse_input(received_message)
            #for debug
            #print(f"Received: {redis_handler.parsed_input.__repr__()}")
            output_redisobject = redis_handler.execute_command(client_id=client_id, input_redisobject=input_redisobject)
            #for debug
            #print(f"Response: {redis_handler.parsed_output.__repr__()}")
            response_message = redis_handler.parse_output(output_redisobject)
            #print(f"Response {response_message} to {address}")
            
            #await asyncio.sleep(3)
            
            writer.write(response_message.encode())
            await writer.drain()
           
             
            if(not redis_handler.buffer.is_empty()):
                print("app.main 88: buffer is not empty", str(redis_handler.buffer))
            
            if(redis_handler.session[client_id]["client_port"] is not None):
                _p = redis_handler.session[client_id]["client_port"]
                print(f"buffer[{_p}] is", str(redis_handler.buffer[_p]))
                if(  not redis_handler.buffer[_p].is_empty()):
                   asyncio.create_task( redis_handler.process_buffer_commands(reader, writer, client_id=client_id))
             
    except:
        #debug
        #print(f"Close connection with {address}")
        writer.close()
        await writer.wait_closed()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, help="The directory where RDB files are stored")
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    parser.add_argument("--port", type=int, help="redis server port number(default to 6379)")
    parser.add_argument("--replicaof", nargs=2, metavar=("MASTER_HOST", "MASTER_PORT"), help="Replicate data from another Redis server")
    
    args = parser.parse_args()
    
    if(args.port is None):
        port_number = 6379
    else:
        port_number = int(args.port)
    
    if(args.replicaof is None):
        redis_server = RedisServerMaster(port_number=port_number)
    else:
        _master_host, _master_port = args.replicaof
        redis_server = RedisServerSlave(port_number=port_number, 
                                        master_host=_master_host, 
                                        master_port=_master_port)
    if(args.dir):
        redis_server.rdb_dir = args.dir
    if(args.dbfilename):
        redis_server.rdb_dbfilename = args.dbfilename
    
    redis_handler = RedisIOHandler(redis_server=redis_server)

    if(args.dir and args.dbfilename):
        asyncio.create_task(import_rdb_file(redis_server))
    server = await asyncio.start_server(lambda r,w: handle_client(r, w, redis_handler), 'localhost', port_number)
    
    if(isinstance(redis_server, RedisServerSlave)):
       # await redis_server.hand_shake()
        replica_reader, replica_writer = await asyncio.open_connection(_master_host,_master_port)
       # redis_replica_handler = RedisIOHandler(redis_server=red)
        replica_comm_task = asyncio.create_task(handle_replica(replica_reader, replica_writer, redis_handler))
        async with server:
            print('Server Running')
            await server.serve_forever()
            await replica_comm_task
    else:
        async with server:
            print('Server Running')
            await server.serve_forever()

if __name__ == "__main__":
    #cProfile.run('asyncio.run(main())')
    asyncio.run(main())

