using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using ChatServer.Core.Interface;
using ChatServer.Core.Packet;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace ChatServer.Core.Network
{
    public class Session : IDisposable
    {
        private static long _nextSessionId;

        private ISessionOwner _owner;
        protected IPacketHandler _packetHandler;

        private Socket _sessionSocket;
        private SocketAsyncEventArgs _readArgs;
        private SocketAsyncEventArgs _writeArgs;

        private readonly RecvBuffer _recvBuffer;
        private readonly Queue<SendBuffer> _sendBuffer;
        private readonly List<SendBuffer> _pendingSendBuffers = new();

        // 세션별 비동기 작업 순서 보장 큐 (같은 세션의 패킷 처리는 도착 순서대로 하나씩만 실행)
        private readonly Queue<Func<ValueTask>> _recvQueue = new();
        private bool _recvQueueRunning;
        private readonly object _recvQueueLock = new object();

        private bool _isConnect;
        private bool _sendRegistered = false;

        private int _receivePending;
        private int _sendPending;
        private int _disconnectRequested;
        private int _released;

        private readonly object _sendLock = new object();

        private string _name ="";

        public Int64 _sessionID { get; private set; }

        public SocketAsyncEventArgs ReadArgs { get { return _readArgs; } }
        public SocketAsyncEventArgs WriteArgs { get { return _writeArgs; } }


        public Session() 
        {
            _recvBuffer = new RecvBuffer();
            _sendBuffer = new Queue<SendBuffer>();
            _isConnect = false;
            _disconnectRequested = 1;
            _released = 1;
        }

        public void Initialize(ISessionOwner owner, IPacketHandler packetHandler, Socket socket, SocketAsyncEventArgs readArgs, SocketAsyncEventArgs writeArgs)
        {
            _owner = owner;
            _packetHandler = packetHandler;

            _sessionSocket = socket;
            _readArgs = readArgs;
            _writeArgs = writeArgs;

            _isConnect = true;

            Volatile.Write(ref _receivePending, 0);
            Volatile.Write(ref _sendPending, 0);
            Volatile.Write(ref _released, 0);
            Volatile.Write(ref _disconnectRequested, 0);

            // SocketAsyncEventArgs 준비.
            readArgs.Completed += IO_Completed;
            readArgs.UserToken = this;
            readArgs.SetBuffer(_recvBuffer.WriteMemory);

            writeArgs.Completed += IO_Completed;
            writeArgs.UserToken = this;

            _sessionID = Interlocked.Increment(ref _nextSessionId);
        }

        public void StartSession()
        {
            if(_readArgs == null)
            {
                Console.WriteLine("session에서 _readArgs가 null입니다. 연결 거부");
                Disconnect();
                return;
            }

            RegisterReceive();
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

        private void RegisterReceive()
        {
            // 이미 종료 요청 상태라면 Receive를 등록하지 않는다.
            if (Volatile.Read(ref _disconnectRequested) != 0)
            {
                TryRelease();
                return;
            }

            // Receive는 동시에 하나만 등록한다.
            if (Interlocked.Exchange(ref _receivePending, 1) != 0)
                throw new InvalidOperationException("Receive 작업이 이미 등록되어 있습니다.");

            // 첫 검사와 pending 설정 사이에 Disconnect가 실행됐는지 재확인한다.
            if (Volatile.Read(ref _disconnectRequested) != 0)
            {
                Volatile.Write(ref _receivePending, 0);

                TryRelease();
                return;
            }

            try
            {
                if (!_sessionSocket.ReceiveAsync(_readArgs))
                    ProcessReceive(_readArgs);
            }
            catch
            {
                Volatile.Write(ref _receivePending, 0);
                Disconnect();
            }
        }

        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            bool receiveAgain = false;

            try
            {
                if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
                {
                    Disconnect();
                    return;
                }

                // 버퍼 데이터 체크
                if (_recvBuffer.OnWrite(e.BytesTransferred) == false)
                {
                    Disconnect();
                    Console.WriteLine("Recv : 0 이여서 disconnect 함");
                    return;
                }

                // 수신된 데이터 처리
                int processLen = OnRecv(_recvBuffer.ReadMemory);

                if (processLen < 0)
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

                receiveAgain = true;
            }
            catch
            {
                Disconnect();
            }
            finally
            {
                Volatile.Write(ref _receivePending, 0);

                if (receiveAgain && Volatile.Read(ref _disconnectRequested) == 0)
                    RegisterReceive();
                else
                    TryRelease();
            }
           
        }

        private void Send(SendBuffer data)
        {
            bool registerSend = false;

            lock (_sendLock)
            {
                if (Volatile.Read(ref _disconnectRequested) != 0)
                {
                    data.Dispose();
                    return;
                }

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

            RegisterSocketSend();
        }

        private void RegisterSocketSend()
        {
            if (Volatile.Read(ref _disconnectRequested) != 0)
            {
                TryRelease();
                return;
            }

            if (Interlocked.Exchange(ref _sendPending, 1) != 0)
                throw new InvalidOperationException("Send 작업이 이미 등록되어 있습니다.");

            try
            {
                if (!_sessionSocket.SendAsync(_writeArgs))
                    ProcessSend(_writeArgs);
            }
            catch
            {
                Volatile.Write(ref _sendPending, 0);
                Disconnect();
            }
        }

        private void ProcessSend(SocketAsyncEventArgs e)
        {
            bool sendRemaining = false;
            bool sendNextBatch = false;

            try
            {
                if (e.SocketError != SocketError.Success || e.BytesTransferred <= 0)
                {
                    Disconnect();
                    return;
                }

                int transferred = e.BytesTransferred;

                while (transferred > 0 && e.BufferList!.Count > 0)
                {
                    ArraySegment<byte> segment = e.BufferList[0];

                    if (transferred >= segment.Count)
                    {
                        transferred -= segment.Count;
                        e.BufferList.RemoveAt(0);

                        SendBuffer completed = _pendingSendBuffers[0];
                        _pendingSendBuffers.RemoveAt(0);
                        completed.Dispose();
                    }
                    else
                    {
                        e.BufferList[0] = new ArraySegment<byte>(
                            segment.Array!,
                            segment.Offset + transferred,
                            segment.Count - transferred);
                        transferred = 0;
                    }
                }

                if (e.BufferList!.Count > 0)
                {
                    sendRemaining = true;
                    return;
                }

                e.BufferList = null;

                lock (_sendLock)
                {
                    sendNextBatch = _sendBuffer.Count > 0;

                    if (!sendNextBatch)
                        _sendRegistered = false;
                }
            }
            catch
            {
                Disconnect();
            }
            finally
            {
                Volatile.Write(ref _sendPending, 0);

                if (Volatile.Read(ref _disconnectRequested) != 0)
                    TryRelease();
                else if (sendRemaining)
                    RegisterSocketSend();
                else if (sendNextBatch)
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

                if (header.Size < PacketHeader.HeaderSize || header.Size > PacketHeader.MaxPacketSize)
                    return -1;

                if (dataSize < header.Size)
                    break;

                ReadOnlySpan<byte> packet = span.Slice(porcessLen, header.Size);

                if (!OnRecvPack(packet))
                {
                    Console.WriteLine($"HandlePacket ERROR - ID : {header.Id}");
                    return -1;
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

        // 세션 전용 비동기 작업 큐: 등록된 작업들을 도착 순서대로 하나씩만 실행한다.
        public void EnqueueAsyncJob(Func<ValueTask> job)
        {
            bool startProcess = false;

            // Race condition을 방지 하기 위해 Lock을 걸어둔다.
            lock (_recvQueueLock)
            {
                // 아직 실행하지 않은 비동기 작업 함수를 Queue에 등록한다.
                _recvQueue.Enqueue(job);

                // 현재 Queue를 처리하는 Drain 작업이 실행 중인지 확인한다.
                if (!_recvQueueRunning)
                {
                    // 실행 중인 Process 작업이 없다면
                    // 현재 호출자가 새로운 Process 작업을 시작하도록 표시한다.
                    _recvQueueRunning = true;
                    startProcess = true;
                }
            }

            // 실행 중인 Process 작업이 없다면 Queue 처리를 시작한다.
            if (startProcess)
            {
                _ = ProcessRecvQueueAsync();
            }
        }

        // Queue에 등록된 비동기 작업을 순차적으로 하나씩 실행한다.
        private async Task ProcessRecvQueueAsync()
        {
            while (true)
            {
                Func<ValueTask> job;

                // race condition 방지 Lock
                lock (_recvQueueLock)
                {
                    // Queue가 비었다면 실행 상태를 해제하고 Process 작업을 종료한다.
                    if (_recvQueue.Count == 0)
                    {
                        _recvQueueRunning = false;
                        return;
                    }

                    // Queue에서 다음에 실행할 비동기 작업 함수를 꺼낸다.
                    job = _recvQueue.Dequeue();
                }

                try
                {
                    // 작업 함수를 호출하고 완료될 때까지 기다린다.
                    // 완료된 후에만 Queue의 다음 작업을 실행한다.
                    await job();
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"세션 비동기 작업 처리 실패: {exception}");
                }
            }
        }

        public void Disconnect()
        {
            if (Interlocked.Exchange(ref _disconnectRequested, 1) != 0)
            {
                return;
            }

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

            TryRelease();
        }

        private void TryRelease()
        {
            if (Volatile.Read(ref _disconnectRequested) == 0 ||
                Volatile.Read(ref _receivePending) != 0 ||
                Volatile.Read(ref _sendPending) != 0)
            {
                return;
            }

            if (Interlocked.Exchange(ref _released, 1) != 0)
                return;

            _owner?.ReleaseSession(this);
        }


        public void Reset()
        {
            _isConnect = false;

            _sessionSocket = null;
            _owner = null;

            _recvBuffer.Reset();

            lock (_recvQueueLock)
            {
                _recvQueue.Clear();
                _recvQueueRunning = false;
            }

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

