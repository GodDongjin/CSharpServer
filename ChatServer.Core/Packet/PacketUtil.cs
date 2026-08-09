using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.Core.Packet
{
    public static class PacketSize
    {
        public const int Bool = 1;
        public const int Byte = 1;
        public const int Int16 = 2;
        public const int UInt16 = 2;
        public const int Int32 = 4;
        public const int UInt32 = 4;
        public const int Int64 = 8;
        public const int UInt64 = 8;
        public const int Float = 4;
        public const int Double = 8;

        public static int String(string value)
        {
            return PacketWriter.GetStringSize(value);
        }
    }

    public ref struct PacketWriter
    {
        private Span<byte> _buffer;
        private Int32 _offset;

        public PacketWriter(Span<byte> buffer)
        {
            _buffer = buffer;
            _offset = 0;
        }

        public Int32 WrittenSize => _offset;

        public void WriteBool(bool value)
        {
            _buffer[_offset++] = value ? (byte)1 : (byte)0;
        }

        public void WriteByte(byte value)
        {
            _buffer[_offset++] = (byte)value;
        }

        public void WriteInt16(Int16 value)
        {
            BinaryPrimitives.WriteInt16LittleEndian(_buffer.Slice(_offset, 2), value);
            _offset += 2;
        }

        public void WriteUInt16(UInt16 value)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_offset, 2), value);
            _offset += 2;
        }

        public void WriteInt32(Int32 value)
        {
            BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_offset, 4), value);
            _offset += 4;
        }

        public void WriteUInt32(UInt32 value)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(_buffer.Slice(_offset, 4), value);
            _offset += 4;
        }

        public void WriteInt64(Int64 value)
        {
            BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_offset, 8), value);
            _offset += 8;
        }

        public void WriteUInt64(UInt64 value)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(_buffer.Slice(_offset, 8), value);
            _offset += 8;
        }

        public void WriteFloat(float value)
        {
            int raw = BitConverter.SingleToInt32Bits(value);
            WriteInt32(raw);
        }

        public void WriteDouble(double value)
        {
            long raw = BitConverter.DoubleToInt64Bits(value);
            WriteInt64(raw);
        }

        public void WriteString(string value)
        {
            int byteCount = Encoding.UTF8.GetByteCount(value);

            if (byteCount > ushort.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(value));

            WriteUInt16((ushort)byteCount);

            Encoding.UTF8.GetBytes(value, _buffer.Slice(_offset, byteCount));
            _offset += byteCount;
        }

        public static Int32 GetStringSize(string value)
        {
            if(value == null)
                value = string.Empty;

            Int32 byteCount = Encoding.UTF8.GetByteCount(value);

            if(byteCount > UInt16.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(value), "string is too long");

            return sizeof(UInt16) + byteCount;
        }
    }

    public ref struct PacketReader
    {
        private ReadOnlySpan<byte> _buffer;
        private Int32 _offset;

        public PacketReader(ReadOnlySpan<byte> buffer)
        {
            _buffer = buffer;
            _offset = 0;
        }

        public bool ReadBool()
        {
            return _buffer[_offset++] != 0;
        }

        public Byte ReadByte()
        {
            return _buffer[_offset++];
        }

        public Int16 ReadInt16()
        {
            Int16 value = BinaryPrimitives.ReadInt16LittleEndian(_buffer.Slice(_offset, 2));
            _offset += 2;
            return value;
        }

        public UInt16 ReadUInt16()
        {
            UInt16 value = BinaryPrimitives.ReadUInt16LittleEndian(_buffer.Slice(_offset, 2));
            _offset += 2;
            return value;
        }

        public Int32 ReadInt32()
        {
            Int32 value = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(_offset, 4));
            _offset += 4;
            return value;
        }

        public UInt32 ReadUInt32()
        {
            UInt32 value = BinaryPrimitives.ReadUInt32LittleEndian(_buffer.Slice(_offset, 4));
            _offset += 4;
            return value;
        }

        public Int64 ReadInt64()
        {
            Int64 value = BinaryPrimitives.ReadInt64LittleEndian(_buffer.Slice(_offset, 8));
            _offset += 8;
            return value;
        }

        public UInt64 ReadUInt64()
        {
            UInt64 value = BinaryPrimitives.ReadUInt64LittleEndian(_buffer.Slice(_offset, 8));
            _offset += 8;
            return value;
        }

        public float ReadFloat()
        {
            Int32 raw = ReadInt32();
            return BitConverter.Int32BitsToSingle(raw);
        }

        public double ReadDouble()
        {
            Int64 raw = ReadInt64();
            return BitConverter.Int64BitsToDouble(raw);
        }

        public string ReadString()
        {
            UInt16 byteCount = ReadUInt16();

            string value = Encoding.UTF8.GetString(_buffer.Slice(_offset, byteCount));
            _offset += byteCount;

            return value;
        }
    }
}
