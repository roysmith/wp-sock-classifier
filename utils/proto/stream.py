"""Read and write varint-delimited proto files.  The file format is
intended to be byte-for-byte identical to that produced by Java's
MessageLite.writeDelimitedTo() method.

"""

class ProtoStreamReader:
    def __init__(self, stream):
        self.stream = stream

    def delimited_protos(self, proto_class):
        """Iterate over delimited protos of type 'proto_class'.

        """
        return self.DelimitedStreamIterator(self, proto_class)

    class DelimitedStreamIterator:
        def __init__(self, reader, proto_class):
            self.reader = reader
            self.proto_class = proto_class
            self.exhausted = False

        def __iter__(self):
            return self

        def __next__(self):
            if self.exhausted:
                raise StopIteration
            proto = self.reader.read_delimited_proto(self.proto_class)
            if proto is None:
                self.exhausted = True
                raise StopIteration
            return proto

    def read_varint(self):
        """Read a varint from the stream.  For details of the encoding, see
        https://developers.google.com/protocol-buffers/docs/encoding#varints

        If stream is initially at EOF, returns None.  Raises
        ValueError if EOF is reached while accumulating bytes and the
        varint has not been properly terminated.

        Per https://github.com/multiformats/unsigned-varint, we
        restrict the length of a varint to 9 bytes.

        """
        bytes = []
        MAX_BYTES = 9
        while True:
            buffer = self.stream.read(1)
            if not buffer:
                if not bytes:
                    return None
                else:
                    raise ValueError("unterminated varint")
            b = buffer[0]
            bytes.append(b & 0x7f)
            if (b & 0x80) == 0:
                break
            if len(bytes) >= 9:
                raise ValueError("too many bytes for varint: %r" % bytes)
        value = 0
        for b in reversed(bytes):
            value = (value << 7) + b
        return value

    def read_proto(self, size, proto_class):
        """Read the next 'size' bytes from the stream.

        Returns a new instance of proto_class, initialized with the
        parsed data.  Proto_class must be of the same type as produced
        the serialized data.

        If stream is initially at EOF, returns None.

        Raises ValueError if EOF is reached before reading 'size' bytes.

        """
        data = self.stream.read(size)
        if not data:
            return None
        if len(data) < size:
            raise ValueError("EOF while reading data")
        return proto_class.FromString(data)


    def read_delimited_proto(self, proto_class):
        """Reads and parses a serialized proto from the stream, preceeded by
        the size of the proto as a varint.  This is intended to be
        wire-compatable with Java's MessageLite.parseDelimitedFrom()
        method.

        Returns a new instance of proto_class, initialized with the
        parsed data.  Proto_class must be of the same type as produced
        the serialized data.

        """
        size = self.read_varint()
        if size is None:
            return None
        proto = self.read_proto(size, proto_class)
        if proto is None:
            raise ValueError("EOF while reading proto data")
        return proto

class ProtoStreamWriter:
    def __init__(self, stream):
        self.stream = stream

    def write_varint(self, i):

        """Write a varint to the stream.  For details of the encoding, see
        https://developers.google.com/protocol-buffers/docs/encoding#varints

        """
        if i < 0:
            raise ValueError("varint cannot be negative")
        buffer = bytearray()
        while True:
            heptet = i & 0x7f
            i >>= 7
            if i:
                buffer.append(heptet | 0x80)
            else:
                buffer.append(heptet)
            if i == 0:
                break
        self.stream.write(buffer)

    def write_proto(self, proto):
        """Write a serialized proto to the stream.

        """
        self.stream.write(proto.SerializeToString())
        

    def write_delimited_proto(self, proto):
        """Write a serialized proto to the stream, preceeded by the size of
        the proto as a varint.  This is intended to be wire-compatable
        with Java's MessageLite.writeDelimitedTo() method.

        """
        data = proto.SerializeToString()
        self.write_varint(len(data))
        self.stream.write(data)
