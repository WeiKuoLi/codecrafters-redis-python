from collections import deque
class BufferMultiQueue(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    def __str__(self):
        _str = ""
        for k in self:
            _str += str(k) +": " + str(self[k]) + ",\n"
        return _str
    def is_empty(self):
        for k in self:
            if (not self[k].is_empty()):
                return False
        return True

class BufferQueue:
    def __init__(self):
        self.buffer = deque()

    def enqueue(self, data):
        self.buffer.append(data)

    def dequeue(self):
        if not self.is_empty():
            return self.buffer.popleft()
        else:
            return None

    def peek(self):
        if not self.is_empty():
            return self.buffer[0]
        else:
            return None

    def is_empty(self):
        return len(self.buffer) == 0

    def size(self):
        return len(self.buffer)
    
    def __repl__(self):
        _str = ""
        for _ in self.buffer:
            _str += " " + _
            
        return _str
    def __str__(self):
        _str = ""
        for _ in self.buffer:
            _str += " " + str(_)
            
        return _str

if __name__=="__main__":
    # Example usage:
    buffer = BufferQueue()

    # Simulating receiving data from a client and adding it to the buffer
    buffer.enqueue("Data1")
    buffer.enqueue("Data2")
    buffer.enqueue("Data3")

    buffer_multiq = BufferMultiQueue()
    buffer_multiq['slave1'] = buffer
    buffer_multiq['slave2'] = buffer
    print(buffer_multiq)
    # Peeking at the front of the buffer
    print("Peek:", buffer.peek())

    # Removing data from the buffer (FIFO)
    print("Dequeue:", buffer.dequeue())
    print("Dequeue:", buffer.dequeue())

    # Checking if the buffer is empty
    print("Is empty:", buffer.is_empty())

    # Size of the buffer
    print("Buffer size:", buffer.size())


    buffer_multiq = BufferMultiQueue()
    buffer_multiq['slave1'] = buffer
    buffer_multiq['slave2'] = buffer
    print(buffer_multiq)
