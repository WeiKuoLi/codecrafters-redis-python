import asyncio

async def handle_client(reader, writer):
    address = writer.get_extra_info('peername')
    print(f"Connected to {address}")

    while True:
        received_data = await reader.read(1024)
        if not received_data:
            break

        print(f"Received {received_data.decode()} from {address}")
        response_message = "+PONG\r\n"
        #await asyncio.sleep(2)
        
        writer.write(response_message.encode())
        await writer.drain()

    print(f"Close connection with {address}")
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, 'localhost', 6379)
    
    async with server:
        print('Server Running')
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())

