using ChatServer.App.Packet;
using ChatServer.Core.Interface;
using ChatServer.Core.Network;
using ChatServer.Core.Packet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.App.ChatSession
{
    public class GameSession : Session
    {
       /* public void SendPacket(ushort packetId, int payloadSize, PacketWriteHandler writePayload)
        {
            SendBuffer sendBuffer = _packetHandler.MakeSendBuffer(packetId, payloadSize, writePayload);
            Send(sendBuffer);
        }*/

        protected override bool OnRecvPack(ReadOnlySpan<byte> packet)
        {
            return _packetHandler.HandlePacket(this, packet);
        }

        //public GameSession() { }
    }
}
