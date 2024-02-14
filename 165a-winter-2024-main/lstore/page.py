
PAGE_SIZE = 4096
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        if len(self.data) < PAGE_SIZE:
            return True
        return False
        pass

    def write(self, value):
        index = self.num_records * 8
        if self.has_capacity():
            # check if remaining space in self.data is enough to store value
            if PAGE_SIZE - len(self.data) <= 8:
                self.data[index : index + 8] = value.to_bytes(8, byteorder='big')
                return index
        return False
        pass
