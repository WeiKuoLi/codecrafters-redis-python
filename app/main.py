import asyncio

class RedisIOHandler:
    def __init__(self):
        self.parsed_input = None
        self.parsed_output = None
        self.redis = {}
    async def delete_key(self, _key, millisecond):
        await asyncio.sleep(millisecond / 1000)
        del self.redis[_key]

    def get_root_object(self, input_string):
        '''
        returns the root object and the rest of unparsed input_string
        '''
        if ((len(input_string)==0 ) or input_string == "\r\n"):
            return None, ""

        head = input_string.split("\r\n")[0]
        assert len(input_string.split("\r\n")) > 0

        if (head[0] == '+'):
            # simple sring
            _str = head[1:]
            _input_string_remove_head = "\r\n".join(input_string.split("\r\n")[1:]) + "\r\n"
            return _str, _input_string_remove_head
        elif (head[0] == '$'):
            # bulk string
            _str_len = int(head[1:])
            _input_string_remove_head = "\r\n".join(input_string.split("\r\n")[1:])+ "\r\n"
            _str = _input_string_remove_head[:_str_len]
            return _str, _input_string_remove_head[_str_len + 2 :]
        elif (head[0] == '*'):
            _lst = []
            _arr_len = int(head[1:])
            _input_string_remove_head = "\r\n".join(input_string.split("\r\n")[1:])+ "\r\n"
            for i in range(_arr_len):
                _node, _str = self.get_root_object(_input_string_remove_head)
                _lst.append(_node)
                _input_string_remove_head = _str
            return _lst, _input_string_remove_head

    def get_resp_string(self, root_obj):
        '''
        from a root_obj return its string
        '''
        if isinstance(root_obj, str):
            return f"${len(root_obj)}\r\n{root_obj}\r\n"
        elif isinstance(root_obj, list):
            _root_obj_len = len(root_obj)
            _str = f"*{_root_obj_len}\r\n"
            for obj in root_obj:
                _str += self.get_resp_string(obj)
            return _str
        return ""

    def render_output_obj(self, input_obj):
        if isinstance(input_obj, str):
            if(input_obj == "ping"):
                return "PONG"
            return input_obj
        elif isinstance(input_obj, list):
            _input_obj_len = len(input_obj)
            if (_input_obj_len == 1):
                return self.render_output_obj(input_obj[0])
            output_obj = []
            _idx = 0
            while _idx < _input_obj_len:
                obj = input_obj[_idx]
                if isinstance(obj, list):
                    output_obj.append(self.render_output_obj(obj))
                elif obj == "ECHO" or obj =="echo":
                    return input_obj[_idx+1]
                    #output_obj.append(input_obj[_idx+1])
                    _idx += 1
                elif obj =="SET" or obj =="set":
                    _key = input_obj[_idx + 1]
                    _value = input_obj[_idx + 2]
                    self.redis[_key] = _value
                    _idx += 2
                    if _idx + 1 < _input_obj_len and input_obj[_idx + 1] == "px":
                        _idx += 2
                        _ps = float(input_obj[_idx+2])
                        asyncio.create_task(self.delete_key(_key, _ps))
                    return "OK"
                elif obj =="GET" or obj =="get":
                    _key = input_obj[_idx + 1]
                    try:
                        _value = self.redis[_key] 
                    except:
                        _value = "error"
                    _idx += 1
                    return _value
                elif obj == "ping":
                    return "PONG"
                    #output_obj.append("PONG")
                _idx += 1
            return output_obj
        return []
    def parse_input(self, input_string):
        # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
        # "*1\r\n$4\r\nping\r\n"
        # "+PONG\r\n"
        root_obj, _ = self.get_root_object(input_string)
        self.parsed_input = root_obj

    def parse_output(self):
        return self.get_resp_string(self.parsed_output)

    def execute_command(self):
        self.parsed_output = self.render_output_obj(self.parsed_input)

async def handle_client(reader, writer):
    redis_handler = RedisIOHandler()
    address = writer.get_extra_info('peername')
    print(f"Connected to {address}")

    while True:
        received_data = await reader.read(1024)
        if not received_data:
            break

        received_message = received_data.decode()
        print(f"Received {received_message} from {address}")

        redis_handler.parse_input(received_message)
        redis_handler.execute_command()
        response_message = redis_handler.parse_output()
        print(f"Response {response_message} to {address}")
        
        # default to response "+PONG\r\n"
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

