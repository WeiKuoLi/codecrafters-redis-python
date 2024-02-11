import argparse
import asyncio
class RedisObject:
    def __init__(self, obj, typ=None):
        self.obj = obj
        if (typ is None):
            if isinstance(obj, int):
                self.typ = "int"
            elif isinstance(obj, str):
                self.typ = "str"
            elif isinstance(obj, list):
                self.typ = "list"
        else:
            self.typ = typ
        self.rdb_dir = "" 
        self.rdb_dbfilename = "" 

    def print(self):
        print(f"{self.typ} object is: {self.obj} ")
    
    def __hash__(self):
        return hash(self.typ + self.obj)

    def __eq__(self, other):
        return isinstance(other, RedisObject) and self.obj == other.obj and self.typ == other.typ

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
            # simple string
            _str = head[1:]
            _input_string_remove_head = "\r\n".join(input_string.split("\r\n")[1:]) + "\r\n"
            return RedisObject(obj=_str, typ="str"), _input_string_remove_head
        elif (head[0] == '$'):
            # bulk string
            _str_len = int(head[1:])
            _input_string_remove_head = "\r\n".join(input_string.split("\r\n")[1:])+ "\r\n"
            _str = _input_string_remove_head[:_str_len]
            return RedisObject(obj=_str, typ="bulk_str" ), _input_string_remove_head[_str_len + 2 :]
        elif (head[0] == '*'):
            _lst = []
            _arr_len = int(head[1:])
            _input_string_remove_head = "\r\n".join(input_string.split("\r\n")[1:])+ "\r\n"
            for i in range(_arr_len):
                _node, _str = self.get_root_object(_input_string_remove_head)
                _lst.append(_node)
                _input_string_remove_head = _str
            return RedisObject(obj=_lst, typ="list"), _input_string_remove_head

    def get_resp_string(self, root_obj):
        '''
        from a root_obj return its string
        '''
        if root_obj.typ =="bulk_str":
            return f"${len(root_obj.obj)}\r\n{root_obj.obj}\r\n"
        elif root_obj.typ == "str":
            return f"+{root_obj.obj}\r\n"
        elif root_obj.typ == "list":
            _root_obj_len = len(root_obj.obj)
            _str = f"*{_root_obj_len}\r\n"
            for node_obj in root_obj.obj:
                _str += self.get_resp_string(node_obj)
            return _str
        elif root_obj.typ == "null_bulk_str":
            return "$-1\r\n"
        return "$-1\r\n"

    def render_output_obj(self, input_obj):
        '''
        returns rendered RedisObject on the parsed_output 
        '''
        if input_obj.typ == "str" or input_obj.typ == "bulk_str":
            if(input_obj.obj == "ping"):
                return RedisObject(obj="PONG", typ="str")
            return input_obj
        elif input_obj.typ == "list":
            _input_obj_len = len(input_obj.obj)
            if (_input_obj_len == 1):
                return self.render_output_obj(input_obj.obj[0])
            output_obj = RedisObject(obj=[], typ="list")
            _idx = 0
            while _idx < _input_obj_len:
                _obj = input_obj.obj[_idx]
                if _obj.typ == "list":
                    output_obj.obj.append(self.render_output_obj(_obj))
                elif _obj.obj == "ECHO" or _obj.obj =="echo":
                    return input_obj.obj[_idx+1]
                    #output_obj.append(input_obj[_idx+1])
                    _idx += 1
                elif _obj.obj =="SET" or _obj.obj =="set":
                    _key = input_obj.obj[_idx + 1]
                    _value = input_obj.obj[_idx + 2]
                    self.redis[_key] = _value
                    print(f"set {_key.typ} {_key.obj},  {_value.typ} {_value.obj} ")
                    _idx += 2
                    if _idx + 1 < _input_obj_len and input_obj.obj[_idx + 1].obj == "px":
                        _ps = float(input_obj.obj[_idx+2].obj)
                        _idx += 2
                        asyncio.create_task(self.delete_key(_key, _ps))
                    return RedisObject(obj="OK", typ="str")
                elif _obj.obj =="GET" or _obj.obj =="get":
                    _key = input_obj.obj[_idx + 1]
                    print(f"get {_key.typ} {_key.obj}")
                    try:
                        _value = self.redis[_key] 
                    except:
                        _value = RedisObject(obj="", typ="null_bulk_str")
                    _idx += 1
                    return _value
                elif _obj.obj == "ping":
                    return RedisObject(obj="PONG", typ="str")
                    #output_obj.append("PONG")
                elif _obj.obj == "CONFIG" or _obj.obj =="config":
                    _config_key =  input_obj.obj[_idx + 2]
                    _config_val = RedisObject(self.rdb_dir) if _config_key.obj == "dir"  else  RedisObject(self.rdb_dbfilename)
                    _output = RedisObject(obj=[])
                    _output.obj.append(_config_key)
                    _output.obj.append(_config_val)
                    return _output
                    _idx += 2
                _idx += 1
            return output_obj
        return RedisObject(obj=[], typ="list")
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

async def handle_client(reader, writer, redis_handler):
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, help="The directory where RDB files are stored")
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    args = parser.parse_args()
    

    redis_handler = RedisIOHandler()
    redis_handler.rdb_dir = args.dir
    redis_handler.rdb_dbfilename = args.dbfilename

    server = await asyncio.start_server(lambda r,w: handle_client(r, w, redis_handler), 'localhost', 6379)
    
    async with server:
        print('Server Running')
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())

