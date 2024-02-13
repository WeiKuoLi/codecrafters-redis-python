
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
            elif isinstance(obj, RedisObject):
                self.typ = "RedisObject"
        else:
            self.typ = typ

    def __repr__(self):
        return (f"< Object {self.obj}, Type: {self.typ} >")
    
    def __str__(self):
        return (f"< Object {str(self.obj)}, Type: {str(self.typ)} >")
    
    def __hash__(self):
        return hash((self.typ, self.obj))

    def __eq__(self, other):
        return isinstance(other, RedisObject) and self.obj == other.obj and self.typ == other.typ
    
    def copy(self):
        return RedisObject(obj=self.obj, typ=self.typ)

