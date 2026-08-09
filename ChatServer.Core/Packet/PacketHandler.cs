using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Buffers.Binary;
using ChatServer.Core.Interface;
using ChatServer.Core.Network;

namespace ChatServer.Core.Packet
{
    // 패킷 ID별 처리 함수를 저장하고 호출하기 위한 delegate 타입
    //public delegate bool PacketHandlerFunc(Session session, ReadOnlySpan<byte> payload);
    public delegate void PacketWriteHandler(Span<byte> payload);

    // 패킷 헤더: [size 2바이트][id 2바이트]
    public readonly struct PacketHeader
    {
        public const ushort HeaderSize = 4;

        public ushort Size { get; }
        public ushort Id { get; }

        public PacketHeader(ushort size, ushort id)
        {
            Size = size;
            Id = id;
        }
    }

    abstract public class PacketHandler : IPacketHandler
    {
        // 패킷 ID별 처리 함수를 저장하는 배열
        //protected readonly PacketHandlerFunc?[] _packetHandler = new PacketHandlerFunc?[ushort.MaxValue + 1];

        // 수신된 전체 패킷에서 헤더를 파싱하고, 패킷 ID에 맞는 처리 함수를 호출한다
        public abstract bool HandlePacket(Session session, ReadOnlySpan<byte> data);

        // 헤더와 payload를 합쳐 송신용 패킷 버퍼를 생성한다.
        public SendBuffer MakeSendBuffer(ushort packetId /*PACKET_ID*/, int payloadSize, PacketWriteHandler writePayload)
        {
            // 전체 패킷 크기 = 헤더 크기 + payload 크기
            int packetSize = PacketHeader.HeaderSize + payloadSize;

            // 패킷 크기는 헤더의 size 필드(ushort)에 저장되므로 범위를 검증한다.
            if (packetSize > ushort.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(payloadSize), "Packet size exceeds ushort.MaxValue.");

            SendBuffer sendBuffer = new SendBuffer(packetSize);
            Memory<byte> buffer = sendBuffer.Buffer;

            // 패킷 헤더 작성: [size 2바이트][id 2바이트]
            BinaryPrimitives.WriteUInt16LittleEndian(buffer.Span.Slice(0, 2), (ushort)packetSize);
            BinaryPrimitives.WriteUInt16LittleEndian(buffer.Span.Slice(2, 2), packetId);

            // 헤더 뒤에 payload 복사
            Span<byte> payload = buffer.Span.Slice(PacketHeader.HeaderSize, payloadSize);
            writePayload(payload);

            // 실제 송신할 데이터 크기를 확정한다.
            sendBuffer.Close(packetSize);

            return sendBuffer;
        }

        // 패킷 처리 함수 등록
        abstract public void Initialize();

        /*private bool HandleReqChat(Session session, ReadOnlySpan<byte> data)
        {
            REQ_CHAT packet = new REQ_CHAT(data);

            Console.WriteLine($"{packet.Name}의 메시지 : {packet.Message}");

            ACK_CHAT ack = new ACK_CHAT(ACK_CHAT.CHAT_STATE.SUCCESS);

            SendBuffer sendBuffer = MakeSendBuffer(
                ACK_CHAT.PacketId,
                ack.GetPayloadSize(),
                ack.WriteData
            );

            session.Send(sendBuffer);

            return true;
        }*/

    }
}
