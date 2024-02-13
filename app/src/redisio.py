
import asyncio
class RedisIOHandler:
    def __init__(self):
        self.parsed_input = None
        self.parsed_output = None
        self.redis = {}
        self.rdb_dir = "" 
        self.rdb_dbfilename = "" 

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
                    _key = input_obj.obj[_idx + 1].obj
                    assert isinstance(_key, str)
                    _value = input_obj.obj[_idx + 2]
                    self.redis[_key] = _value
                    print(f"set {str(_key)}: {str(_value)} ")
                    _idx += 2
                    if _idx + 1 < _input_obj_len and input_obj.obj[_idx + 1].obj == "px":
                        _ps = float(input_obj.obj[_idx+2].obj)
                        _idx += 2
                        asyncio.create_task(self.delete_key(_key, _ps))
                    return RedisObject(obj="OK", typ="str")
                elif _obj.obj =="GET" or _obj.obj =="get":
                    _key = input_obj.obj[_idx + 1].obj
                    assert isinstance(_key, str)
                    print(f"get {str(_key)}")
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
                    _idx += 2
                    return _output
                elif _obj.obj == "keys":
                    _output = RedisObject(obj=[])
                    for _key in self.redis:
                        _output.obj.append(RedisObject(obj=_key, typ="bulk_str"))
                    _idx += 1
                    if(len(_output.obj) ==0 ):
                        return RedisObject(obj="",typ="null_bulk_str")
                    return _output

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