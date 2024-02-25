import cProfile

import argparse
import asyncio
import uuid

from app.src.redisdata import RedisObject
from app.src.redisio import RedisIOHandler
from app.src.redis_slave import RedisServerSlave
from app.src.redis_master import RedisServerMaster

from app.src.rdb import import_rdb_file

async def handle_master(reader, writer, redis_handler):
    client_id = str(uuid.uuid4()) 
    while (client_id in redis_handler.session):
        client_id = str(uuid.uuid4()) 
    
    redis_handler.session[client_id] = { "reader":reader,
                                         "writer": writer,
                                         "client_ip":None,
                                         "client_port":None}
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
                print(f"recived {len(received_message)} bytes: ")
                print(f"{RedisObject.from_string(received_message).__repr__()} from master server")
            except:
                received_message = "+ping\r\n"
                print("received message that cannot be decoded")

            input_redisobject = redis_handler.parse_input(received_message)
            try:
                output_redisobject = redis_handler.execute_master_command(client_id=client_id, input_redisobject=input_redisobject)
                print(output_redisobject.__repr__())
            except Exception as e:
                print(f"cannot excecute command, {e}")
            #for debug
            #print(f"Response master: {redis_handler.parsed_output.__repr__()}")
            
            #response_message = redis_handler.parse_output(output_redisobject)
           # print(f"Response master {response_message} to {address}")
            
            
          #  writer.write(response_message.encode())
          #  await writer.drain()

    except:
        print("app.main.handle_master error")
        #debug
    finally:
        print(f"Close connection with MASTER")
        writer.close()
        await writer.wait_closed()


async def handle_general_client(reader, writer, redis_handler):
    client_id = str(uuid.uuid4()) 
    while (client_id in redis_handler.session):
        client_id = str(uuid.uuid4()) 
    
    redis_handler.session[client_id] = { "reader":reader,
                                         "writer": writer,
                                         "client_ip":None,
                                         "client_port":None,
                                         "is_replica":False}
    
    #debug
    #print(f"Connected to {client_id}")
    try:
        for _round in range(5): # determine whether it is replica within 5 interactions
            received_data = await reader.read(1024)
            if not received_data:
                break
            
            received_message = received_data.decode()
            print(f"Received {received_message} from {client_id}")

            input_redisobject = redis_handler.parse_input(received_message)
            #for debug
            print(f"Received: {input_redisobject.__repr__()}")
            output_redisobject = redis_handler.execute_command(client_id=client_id, input_redisobject=input_redisobject)
            #for debug
            print(redis_handler.redis_server.redis)
            print(f"Response: {output_redisobject.__repr__()}")
            response_message = redis_handler.parse_output(output_redisobject)
            print(f"Response {response_message} to {client_id}")
            
            #await asyncio.sleep(3)
            
            writer.write(response_message.encode())
            await writer.drain()
            
            print("message sent")
            
            if(redis_handler.session[client_id]["is_replica"] ):
                if (not redis_handler.buffer[client_id].is_empty()):
                    ## handshake finish, enter handle_replica 
                    break
        # handle interaction according to redis_handler.session[client_id]["is_replica"]
        if(redis_handler.session[client_id]["is_replica"]):
            await handle_replica(client_id, reader, writer, redis_handler)
        else:
            print('handle_normal_client')
            await handle_normal_client(client_id, reader, writer, redis_handler )
    except:
        print(f"connection with {client_id} encounter error")
    finally:
        if(redis_handler.session[client_id]["is_replica"]):
            _p = redis_handler.session[client_id]["client_port"]
            print(f"replica client {client_id} connection end")
            print(f"connection with replica@{_p} end")

            print(f"buffer[{client_id}] is", str(redis_handler.buffer[client_id]))
        #debug
        print(f"~~~Closing connection~~~ ")
        writer.close()
        await writer.wait_closed()

async def handle_replica(client_id, reader, writer, redis_handler):
    '''
    We are a master talking to a replica, do not wait reponse before command
    '''
    print(f"buffer[{client_id}] is", str(redis_handler.buffer[client_id]))
    
    while True:
        print("handle_replica...")
        if(redis_handler.buffer[client_id].is_empty()):
            await asyncio.sleep(.2)
            pass
        else:
            print(f"buffer[{client_id}] is", str(redis_handler.buffer[client_id]))
            await ( redis_handler.process_buffer_commands(reader, writer, client_id=client_id))
        
        '''
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
        '''
         
    
async def handle_normal_client(client_id, reader, writer, redis_handler):
    while True:
        print("handle normal client...")
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
        
        #print("reply sent")

             

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
    server = await asyncio.start_server(lambda r,w: handle_general_client(r, w, redis_handler), 'localhost', port_number)
    
    if(isinstance(redis_server, RedisServerSlave)):
       # await redis_server.hand_shake()
        replica_reader, replica_writer = await asyncio.open_connection(_master_host,_master_port)
       # redis_replica_handler = RedisIOHandler(redis_server=red)
        replica_comm_task = asyncio.create_task(handle_master(replica_reader, replica_writer, redis_handler))
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

