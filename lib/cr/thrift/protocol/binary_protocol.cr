require "./base_protocol.cr"

module Thrift
  class BinaryProtocol < BaseProtocol
    VERSION_MASK = 0xffff0000
    VERSION_1 = 0x80000000
    TYPE_MASK = 0x000000ff

    getter :strict_read, :strict_write

    def initialize(trans, @strict_read=true, @strict_write=true)
      super(trans)
      # Pre-allocated read buffer for fixed-size read methods. Needs to be at least 8 bytes long for
      # read_i64() and read_double().
      @rbuf = Bytes.new(8, 0)
    end

    def write_message_begin(name, type, seqid)
      # this is necessary because we added (needed) bounds checking to 
      # write_i32, and 0x80010000 is too big for that.
      if strict_write
        write_i16(VERSION_1 >> 16)
        write_i16(type)
        write_string(name)
        write_i32(seqid)
      else
        write_string(name)
        write_byte(type)
        write_i32(seqid)
      end
    end

    def write_struct_begin(name); nil; end

    def write_field_begin(name, type, id)
      write_byte(type)
      write_i16(id)
    end

    def write_field_stop
      write_byte(Thrift::Types::STOP)
    end

    def write_map_begin(ktype, vtype, size)
      write_byte(ktype)
      write_byte(vtype)
      write_i32(size)
    end

    def write_list_begin(etype, size)
      write_byte(etype)
      write_i32(size)
    end

    def write_set_begin(etype, size)
      write_byte(etype)
      write_i32(size)
    end

    def write_bool(bool)
      write_byte(bool ? 1 : 0)
    end

    def write_byte(byte)
      raise RangeError.new "" if byte < -2**31 || byte >= 2**32
      raw = uninitialized Uint8[1]
      IO::ByteFormat::BigEndian.encode(byte, raw.to_slice)
      trans.write(raw)
    end

    def write_i16(i16)
      raw = uninitialized Uint8[2]
      IO::ByteFormat::BigEndian.encode(i16, raw.to_slice)
      trans.write(raw)
    end

    def write_i32(i32)
      raise RangeError.new "" if i32 < -2**31 || i32 >= 2**31
      raw = uninitialized Uint8[4]
      IO::ByteFormat::BigEndian.encode(i32, raw.to_slice)
      trans.write(raw)
    end

    def write_i64(i64)
      raise RangeError.new "" if i64 < -2**63 || i64 >= 2**64
      raw = uninitialized Uint8[8]
      IO::ByteFormat::BigEndian.encode(i64, raw.to_slice)
      trans.write(raw)
    end

    def write_double(dub)
      raw = uninitialized Uint8[8]
      IO::ByteFormat::BigEndian.encode(dub, raw.to_slice)
      trans.write(raw)     
    end

    def write_string(str)
      buf = str.encode("utf-8")
      write_binary(buf)
    end

    def write_binary(buf)
      write_i32(buf.size)
      trans.write(buf)
    end

    def read_message_begin
      version = read_i32
      if version < 0
        if (version & VERSION_MASK != VERSION_1)
          raise ProtocolException.new(ProtocolException::BAD_VERSION, "Missing version identifier")
        end
        type = version & TYPE_MASK
        name = read_string
        seqid = read_i32
        [name, type, seqid]
      else
        if strict_read
          raise ProtocolException.new(ProtocolException::BAD_VERSION, "No version identifier, old protocol client?")
        end
        name = trans.read_all(version)
        type = read_byte
        seqid = read_i32
        [name, type, seqid]
      end
    end

    def read_struct_begin; nil; end

    def read_field_begin
      type = read_byte
      if (type == Types::STOP)
        [nil, type, 0]
      else
        id = read_i16
        [nil, type, id]
      end
    end

    def read_map_begin
      ktype = read_byte
      vtype = read_byte
      size = read_i32
      [ktype, vtype, size]
    end

    def read_list_begin
      etype = read_byte
      size = read_i32
      [etype, size]
    end

    def read_set_begin
      etype = read_byte
      size = read_i32
      [etype, size]
    end

    def read_bool
      byte = read_byte
      byte != 0
    end

    def read_byte
      val = trans.read_byte
      if (val > 0x7f)
        val = 0 - ((val - 1) ^ 0xff)
      end
      val
    end

    def read_i16
      trans.read_into_buffer(@rbuf, 2)
      val = IO::ByteFormat::BigEndian.decode(Int16, @rbuf)
    end

    def read_i32
      trans.read_into_buffer(@rbuf, 4)
      val = IO::ByteFormat::BigEndian.decode(Int32, @rbuf)
    end

    def read_i64
      trans.read_into_buffer(@rbuf, 8)
      val = IO::ByteFormat::BigEndian.decode(Int64, @rbuf)
    end

    def read_double
      trans.read_into_buffer(@rbuf, 8)
      val = IO::ByteFormat::BigEndian.decode(Float64, @rbuf)
    end

    def read_string
      buffer = read_binary
      String.new(buffer)
    end

    def read_binary
      size = read_i32
      trans.read_all(size)
    end
    
    def to_s
      "binary(#{super.to_s})"
    end
  end

  class BinaryProtocolFactory < BaseProtocolFactory
    def get_protocol(trans)
      return Thrift::BinaryProtocol.new(trans)
    end
    
    def to_s
      "binary"
    end


  end
end