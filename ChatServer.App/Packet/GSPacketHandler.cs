using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ChatServer.App.ChatSession;
using ChatServer.Core.Interface;
using ChatServer.Core.Network;
using ChatServer.Core.Packet;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace ChatServer.App.Packet
{
    // 패킷 ID별 처리 함수를 저장하고 호출하기 위한 delegate 타입
    public delegate bool PacketHandlerFunc(GameSession session, ReadOnlySpan<byte> payload);

    // 패킷 ID
    public enum PACKET_ID : ushort
    {
        PKT_REQ_CHAT = 1000,
        PKT_ACK_CHAT = 1001,
        PKT_REQ_GET_ROOM_INFO = 1002,
        PKT_ACK_GET_ROOM_INFO = 1003,
    }

    public sealed class GSPacketHandler : PacketHandler
    {
        private readonly PacketHandlerFunc?[] _packetHandler = new PacketHandlerFunc?[ushort.MaxValue + 1];


        public override void Initialize()
        {
            // 패킷 ID에 대응하는 처리 함수를 등록한다.
            _packetHandler[(ushort)PACKET_ID.PKT_REQ_CHAT] = HandleReqChat;
        }

        override public bool HandlePacket(Session session, ReadOnlySpan<byte> data)
        {
            if (session is not GameSession gameSession)
                return false;

            // 최소 헤더 크기 확인
            if (data.Length < PacketHeader.HeaderSize)
                return false;

            // 패킷 헤더 파싱: [size 2바이트][id 2바이트]
            ushort size = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(0, 2));
            ushort id = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(2, 2));

            // 패킷 크기 검증
            if (size < PacketHeader.HeaderSize || size > data.Length)
                return false;

            PacketHeader header = new PacketHeader(size, id);

            // 패킷 ID에 맞는 처리 함수 조회
            PacketHandlerFunc? handler = _packetHandler[header.Id];
            if (handler == null)
                return false;

            // 헤더를 제외한 payload 영역 추출.
            ReadOnlySpan<byte> payload = data.Slice(PacketHeader.HeaderSize, header.Size - PacketHeader.HeaderSize);

            // 패킷 처리 함수 호출
            return handler(gameSession, payload);
        }

        private bool HandleReqChat(GameSession session, ReadOnlySpan<byte> data)
        {
            REQ_CHAT packet = new REQ_CHAT(data);
            string name = packet.Name;
            string message = packet.Message;

            // 수신 버퍼가 재사용되기 전에 패킷을 파싱한 뒤,
            // 실제 처리는 Session의 비동기 작업 Queue에 등록한다.
            session.EnqueueAsyncJob(
                () => HandleReqChatAsync(session, name, message));

            return true;
        }

        private static ValueTask HandleReqChatAsync(
            GameSession session,
            string name,
            string message)
        {

            Console.WriteLine($"{name}의 메시지 : {message}");

            ACK_CHAT ack = new ACK_CHAT(CHAT_STATE.SUCCESS);

            session.SendPacket(
                (ushort)ACK_CHAT.PacketId,
                ack.GetPayloadSize(),
                ack.WriteData);

            return ValueTask.CompletedTask;
        }
    }
}
