import argparse
import asyncio
from .src.redis import RedisObject
from .src.redisio import RedisIOHandler
from .rdb import import_rdb_file

async def handle_client(reader, writer, redis_handler):
    address = writer.get_extra_info('peername')
    print(f"Connected to {address}")

    while True:
        received_data = await reader.read(1024)
        if not received_data:
            break

        received_message = received_data.decode()
        #print(f"Received {received_message} from {address}")

        redis_handler.parse_input(received_message)
        print(f"Received: {redis_handler.parsed_input}")
        redis_handler.execute_command()
        print(f"Response: {redis_handler.parsed_output}")
        response_message = redis_handler.parse_output()
        #print(f"Response {response_message} to {address}")
        
        # default to response "+PONG\r\n"
        #await asyncio.sleep(2)
        
        writer.write(response_message.encode())
        await writer.drain()

    print(f"Close connection with {address}")
    writer.close()
    await writer.wait_closed()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, help="The directory where RDB files are stored")
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    args = parser.parse_args()
    

    redis_handler = RedisIOHandler()
    redis_handler.rdb_dir = args.dir
    redis_handler.rdb_dbfilename = args.dbfilename
    
    await import_rdb_file(redis_handler) 
    server = await asyncio.start_server(lambda r,w: handle_client(r, w, redis_handler), 'localhost', 6379)
    
    async with server:
        print('Server Running')
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())

