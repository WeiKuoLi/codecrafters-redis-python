import cProfile

import argparse
import asyncio
import uuid

from app.src.redisdata import RedisObject
from app.src.redisio import RedisIOHandler
from app.src.redis_slave import RedisServerSlave
from app.src.redis_master import RedisServerMaster

from app.src.rdb import import_rdb_file


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
        
        # default to response "+PONG\r\n"
        #await asyncio.sleep(2)
        
        writer.write(response_message.encode())
        await writer.drain()
       

        if(redis_handler.session[client_id]["client_port"] is not None):
            _p = redis_handler.session[client_id]["client_port"]
            if(not redis_handler.buffer[_p].is_empty()):
                await redis_handler.process_buffer_commands(reader, writer, client_id=client_id)


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
        await redis_server.hand_shake()
    if(args.dir):
        redis_server.rdb_dir = args.dir
    if(args.dbfilename):
        redis_server.rdb_dbfilename = args.dbfilename
    
    redis_handler = RedisIOHandler(redis_server=redis_server)

    if(args.dir and args.dbfilename):
        asyncio.create_task(import_rdb_file(redis_server))
    server = await asyncio.start_server(lambda r,w: handle_client(r, w, redis_handler), 'localhost', port_number)
    
    async with server:
        print('Server Running')
        await server.serve_forever()

if __name__ == "__main__":
    #cProfile.run('asyncio.run(main())')
    asyncio.run(main())

