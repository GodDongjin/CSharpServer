using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace C__Server_Study
{
    public struct PakcetHeader
    {
        public int Length;
        public ushort Id;
    }

    public interface IPacket
    {
        ushort Id { get; }
        void Serialize(BinaryWriter wirter);
        void Deserialize(BinaryReader reader);
    }

    // 패킷 수현 예제 - 채팅 메시지
    public class ChatMessagePacket : IPacket
    {
        public ushort Id => 101;

        public string Sender { get; set; }
        public string Message { get; set; }

        public void Serialize(BinaryWriter wirter)
        {
            wirter.Write(Sender);
            wirter.Write(Message);
        }
        public void Deserialize(BinaryReader reader)
        {
            Sender = reader.ReadString();
            Message = reader.ReadString();
        }
    }
}
