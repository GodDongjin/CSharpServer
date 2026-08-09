using ChatServer.Core.Interface;
using ChatServer.Core.Packet;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace ChatServer.Core.Network
{
    public class Session : IDisposable
    {
        private ISessionOwner _owner;
        protected IPacketHandler _packetHandler;

        private Socket _sessionSocket;
        private SocketAsyncEventArgs _readArgs;
        private SocketAsyncEventArgs _writeArgs;

        private readonly RecvBuffer _recvBuffer;
        private readonly Queue<SendBuffer> _sendBuffer;
        private readonly List<SendBuffer> _pendingSendBuffers = new();

        private bool _isConnect;
        private bool _disconnected;
        private bool _sendRegistered = false;

        private readonly object _sendLock = new object();

        private string _name ="";
        public Guid _id{ get; } = Guid.NewGuid();

        public SocketAsyncEventArgs ReadArgs { get { return _readArgs; } }
        public SocketAsyncEventArgs WriteArgs { get { return _writeArgs; } }


        public Session() 
        {
            _recvBuffer = new RecvBuffer();
            _sendBuffer = new Queue<SendBuffer>();
            _isConnect = false;
            _disconnected = false;
        }

        public void Initialize(ISessionOwner owner, IPacketHandler packetHandler, Socket socket, SocketAsyncEventArgs readArgs, SocketAsyncEventArgs writeArgs)
        {
            _owner = owner;
            _packetHandler = packetHandler;

            _sessionSocket = socket;
            _readArgs = readArgs;
            _writeArgs = writeArgs;

            _isConnect = true;
            _disconnected = false;


            // SocketAsyncEventArgs 준비.
            readArgs.Completed += IO_Completed;
            readArgs.UserToken = this;
            readArgs.SetBuffer(_recvBuffer.WriteMemory);

            writeArgs.Completed += IO_Completed;
            writeArgs.UserToken = this;
        }

        public void StartSession()
        {
            if(_readArgs == null)
            {
                Console.WriteLine("session에서 _readArgs가 null입니다. 연결 거부");
                _sessionSocket.Close();
            }

            bool pending = _sessionSocket.ReceiveAsync(_readArgs);
            if (!pending)
                ProcessReceive(_readArgs);
        }

        private void IO_Completed(object? sender, SocketAsyncEventArgs e)
        {
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    ProcessReceive(e);
                    break;
                case SocketAsyncOperation.Send:
                    ProcessSend(e);
                    break;
                default:
                    throw new ArgumentException("지원되지 않는 작업 유형");
            }
        }

        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            if(e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                Disconnect();
                return;
            }

            // 버퍼 데이터 체크
            if(_recvBuffer.OnWrite(e.BytesTransferred) == false)
            {
                Disconnect();
                Console.WriteLine("Recv : 0 이여서 disconnect 함");
                return;
            }

            // 수신된 데이터 처리
            int processLen = OnRecv(_recvBuffer.ReadMemory);

            if(processLen < 0)
            {
                Disconnect();
                Console.WriteLine("processLen is 0");
                return;
            }

            if (!_recvBuffer.OnRead(processLen))
            {
                Disconnect();
                Console.WriteLine("RecvBuffer read failed.");
                return;
            }

            // recv 버퍼 다 읽었으니 버퍼 갱신.
            _recvBuffer.Clear();

            // SocketAsyncEventArgs에 버퍼 갱신.
            e.SetBuffer(_recvBuffer.WriteMemory);

            // 다시 수신 대기
            bool wileRaiseEvent = _sessionSocket.ReceiveAsync(e);
            if (!wileRaiseEvent)
            {
                ProcessReceive(e);
            }
        }

        public void Send(SendBuffer data)
        {
            bool registerSend = false;

            lock (_sendLock)
            {
                _sendBuffer.Enqueue(data);

                if (_sendRegistered == false)
                {
                    _sendRegistered = true;
                    registerSend = true;
                }
            }

            if (registerSend)
            {
                RegisterSend();
            }
        }

        private void RegisterSend()
        {
            if(_writeArgs == null)
            {
                lock(_sendLock)
                {
                    _sendRegistered = false;
                }

                // SocketAsyncEventArgs가 NULL이여서 오류 로그 남김
                Console.WriteLine("송신 SocketAsyncEventArgs객체가 없습니다. 연결해제");
                Disconnect();
                return;
            }

            _pendingSendBuffers.Clear();

           
            lock(_sendLock)
            {
                while(_sendBuffer.Count > 0)
                {
                    SendBuffer sendBuffer = _sendBuffer.Dequeue();

                    _pendingSendBuffers.Add(sendBuffer);
                }
            }

            List<ArraySegment<byte>> bufferList = new List<ArraySegment<byte>>();

            foreach (SendBuffer sendBuffer in _pendingSendBuffers)
            {
                bufferList.Add(sendBuffer.ToArraySegment());
            }

            _writeArgs.BufferList = bufferList;

            // 비동기 전송 시작
            bool willRaiseEvent = _sessionSocket.SendAsync(_writeArgs);
            if (!willRaiseEvent)
            {
                ProcessSend(_writeArgs);
            }
        }

        private void ProcessSend(SocketAsyncEventArgs e)
        {
            try
            {
                if (e.SocketError != SocketError.Success || e.BytesTransferred <= 0)
                {
                    Disconnect();
                    return;
                }
            }
            finally
            {
                foreach (SendBuffer sendBuffer in _pendingSendBuffers)
                {
                    sendBuffer.Dispose();
                }

                _pendingSendBuffers.Clear();
                e.BufferList = null;

            }

            bool registerSend = false;

            lock (_sendLock)
            {
                if (_sendBuffer.Count > 0)
                {
                    registerSend = true;
                }
                else
                {
                    _sendRegistered = false;
                }
            }

            if (registerSend)
            {
                RegisterSend();
            }
        }

        private Int32 OnRecv(ReadOnlyMemory<byte> buffer)
        {
            Int32 porcessLen = 0;

            while (true)
            {
                Int32 dataSize = buffer.Length - porcessLen;

                if (dataSize < PacketHeader.HeaderSize)
                {
                    break;
                }

                ReadOnlySpan<byte> span = buffer.Span;

                ushort size = BinaryPrimitives.ReadUInt16LittleEndian(buffer.Span.Slice(porcessLen, 2));
                ushort id = BinaryPrimitives.ReadUInt16LittleEndian(buffer.Span.Slice(porcessLen + 2, 2));

                PacketHeader header = new PacketHeader(size, id);

                if (dataSize < header.Size)
                    break;

                if (header.Size < PacketHeader.HeaderSize)
                    return -1;

                ReadOnlySpan<byte> packet = span.Slice(porcessLen, header.Size);

                if (!OnRecvPack(packet))
                {
                    Console.WriteLine($"HandlePacket ERROR - ID : {header.Id}");
                }

                porcessLen += header.Size;
            }

            return porcessLen;
        }

        public void SendPacket(ushort packetId, int payloadSize, PacketWriteHandler writePayload)
        {
            SendBuffer sendBuffer = _packetHandler.MakeSendBuffer(packetId, payloadSize, writePayload);
            Send(sendBuffer);
        }

        protected virtual bool OnRecvPack(ReadOnlySpan<byte> packet)
        {
            return _packetHandler.HandlePacket(this, packet);
        }

        public void Disconnect()
        {
            if (_disconnected) {
                return;
            }

            _disconnected = true;
            _isConnect = false;

            try
            {
                _sessionSocket?.Shutdown(SocketShutdown.Both);
            }
            catch
            {
            }

            try
            {
                _sessionSocket?.Close();
            }
            catch
            {
            }

            _owner?.ReleaseSession(this);
        }


        public void Reset()
        {
            _isConnect = false;
            _disconnected = false;

            _sessionSocket = null;
            _owner = null;

            _recvBuffer.Reset();

            lock (_sendLock)
            {
                while (_sendBuffer.Count > 0)
                    _sendBuffer.Dequeue().Dispose();

                foreach (SendBuffer sendBuffer in _pendingSendBuffers)
                    sendBuffer.Dispose();

                _pendingSendBuffers.Clear();
                _sendRegistered = false;
            }

            _readArgs.Completed -= IO_Completed;
            _writeArgs.Completed -= IO_Completed;

            _readArgs.UserToken = null;
            _writeArgs.UserToken = null;

            _readArgs.SetBuffer(Memory<byte>.Empty);
            _writeArgs.BufferList = null;
        }

        public void Dispose()
        {
            _isConnect = false;

            _sessionSocket?.Close();
            _readArgs?.Dispose();
            _writeArgs?.Dispose();
            _recvBuffer.Dispose();
        }

      
    }
}
