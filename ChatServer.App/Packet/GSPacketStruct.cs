using ChatServer.Core.Packet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.App.Packet
{
    public readonly ref struct REQ_CHAT
    {
        public const PACKET_ID PacketId = PACKET_ID.PKT_REQ_CHAT;

        public string Name { get; }
        public string Message { get; }

        public REQ_CHAT(ReadOnlySpan<byte> buffer)
        {
            PacketReader packetReader = new PacketReader(buffer);

            Name = packetReader.ReadString();
            Message = packetReader.ReadString();
        }
    }

    public readonly struct ACK_CHAT
    {
        public const PACKET_ID PacketId = PACKET_ID.PKT_ACK_CHAT;

        public CHAT_STATE State { get; }

        public ACK_CHAT(CHAT_STATE result)
        {
            State = result;
        }

        public int GetPayloadSize()
        {
            return PacketSize.UInt16;
        }

        public void WriteData(Span<byte> data)
        {
            PacketWriter writer = new PacketWriter(data);

            writer.WriteUInt16((UInt16)State);
        }
    }

    public readonly ref struct REQ_GET_ROOM_INFO
    {
        public const PACKET_ID PacketId = PACKET_ID.PKT_REQ_GET_ROOM_INFO;

        public REQ_GET_ROOM_INFO(ReadOnlySpan<byte> buffer)
        {
            PacketReader packetReader = new PacketReader(buffer);
        }
    }

    public readonly struct ACK_GET_ROOM_INFO
    {
        public const PACKET_ID PacketId = PACKET_ID.PKT_ACK_GET_ROOM_INFO;

        public GET_ROOM_INFO_STATE State { get; }

        public ACK_GET_ROOM_INFO(GET_ROOM_INFO_STATE result)
        {
            State = result;
        }

        public int GetPayloadSize()
        {
            return PacketSize.UInt16;
        }

        public void WriteData(Span<byte> data)
        {
            PacketWriter writer = new PacketWriter(data);

            writer.WriteUInt16((UInt16)State);
        }
    }
}
