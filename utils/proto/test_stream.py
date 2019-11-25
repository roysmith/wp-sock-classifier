import unittest
from io import BytesIO, SEEK_SET
from stream import ProtoStreamReader, ProtoStreamWriter
from google.protobuf.api_pb2 import Method

# Note: Some of these tests use a Method proto.  There's nothing
# special about Method; it's just a Google well-known type so it's
# easily available for testing.

def make_stream(*bytes):
    return BytesIO(bytearray(bytes))

class ProtoStreamReaderTest(unittest.TestCase):
    def test_construct(self):
        ProtoStreamReader(BytesIO())

    def test_read_varint_zero(self):
        reader = ProtoStreamReader(make_stream(0x0))
        self.assertEqual(reader.read_varint(), 0)

    def test_read_varint_one_byte(self):
        # Example from developers.google.com/protocol-buffers/docs/encoding#types
        reader = ProtoStreamReader(make_stream(0x1))
        self.assertEqual(reader.read_varint(), 1)

    def test_read_varint_two_bytes(self):
        # Example from developers.google.com/protocol-buffers/docs/encoding#types
        reader = ProtoStreamReader(make_stream(0xac, 0x02))
        self.assertEqual(reader.read_varint(), 300)

    def test_read_varint_nine_bytes_succeeds(self):
        reader = ProtoStreamReader(make_stream(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00))
        reader.read_varint()


    def test_read_varint_ten_bytes_raises_value_error(self):
        reader = ProtoStreamReader(make_stream(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00))
        with self.assertRaises(ValueError):
            reader.read_varint()

    def test_read_varint_returns_none_on_eof(self):
        reader = ProtoStreamReader(make_stream(0x01))
        self.assertEqual(reader.read_varint(), 1)
        self.assertIsNone(reader.read_varint())

    def test_read_varint_raises_value_error_on_unterminated_byte_sequence(self):
        reader = ProtoStreamReader(make_stream(0x80))
        with self.assertRaises(ValueError):
            reader.read_varint()

    def test_read_proto_returns_none_on_eof(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)
        method1 = Method()
        method1.name = "foo"
        size = method1.ByteSize()
        writer.write_proto(method1)
        stream.seek(0, SEEK_SET)
        method2 = reader.read_proto(size, Method)
        self.assertEqual(method1, method2)
        method3 = reader.read_proto(size, Method)
        self.assertIsNone(method3)

    def test_read_proto_raises_value_error_on_early_eof(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)
        method1 = Method()
        method1.name = "foo"
        size = method1.ByteSize()
        writer.write_proto(method1)
        stream.seek(0, SEEK_SET)
        with self.assertRaises(ValueError):
            reader.read_proto(size+1, Method)

    def test_read_delimited_proto_returns_none_on_eof(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)
        method1 = Method()
        method1.name = "foo"
        writer.write_delimited_proto(method1)
        stream.seek(0, SEEK_SET)
        method2 = reader.read_delimited_proto(Method)
        self.assertEqual(method1, method2)
        self.assertIsNone(reader.read_delimited_proto(Method))

    def test_iterator(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)
        method1 = Method()
        method1.name = "m1"
        method2 = Method()
        method2.name = "m2"
        method3 = Method()
        method3.name = "m3"
        writer.write_delimited_proto(method1)
        writer.write_delimited_proto(method2)
        writer.write_delimited_proto(method3)
        stream.seek(0, SEEK_SET)
        protos = [p for p in reader.delimited_protos(Method)]
        self.assertEqual(protos, [method1, method2, method3])

        
class ProtoStreamWriterTest(unittest.TestCase):
    def test_construct(self):
        ProtoStreamWriter(BytesIO())

    def test_write_varint_zero(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)

        writer.write_varint(0)
        stream.seek(0, SEEK_SET)
        self.assertEqual(reader.read_varint(), 0)

    def test_write_varint_one_byte(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)

        writer.write_varint(1)
        stream.seek(0, SEEK_SET)
        self.assertEqual(reader.read_varint(), 1)

    def test_write_varint_two_bytes(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)
        writer.write_varint(300)
        stream.seek(0, SEEK_SET)
        self.assertEqual(reader.read_varint(), 300)

    def test_write_negative_number_raises_value_error(self):
        stream = BytesIO()
        writer = ProtoStreamWriter(stream)
        with self.assertRaises(ValueError):
            writer.write_varint(-1)

    def test_write_proto(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)
        method1 = Method()
        method1.name = "foo"
        method1.request_streaming = True
        size = method1.ByteSize()
        writer.write_proto(method1)
        stream.seek(0, SEEK_SET)
        method2 = reader.read_proto(size, Method)
        self.assertEqual(method1, method2)

    def test_write_delimited_proto(self):
        stream = BytesIO()
        reader = ProtoStreamReader(stream)
        writer = ProtoStreamWriter(stream)
        method1 = Method()
        method1.name = "foo"
        method1.request_streaming = True
        writer.write_delimited_proto(method1)
        stream.seek(0, SEEK_SET)
        method2 = reader.read_delimited_proto(Method)
        self.assertEqual(method1, method2)

