using ChatServer.Core.Network;
using ChatServer.Core.Packet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.Core.Interface
{
    public interface IPacketHandler
    {
        void Initialize();
        SendBuffer MakeSendBuffer(ushort packetId, int payloadSize, PacketWriteHandler writePayload);
        bool HandlePacket(Session session, ReadOnlySpan<byte> data);
    }
}
