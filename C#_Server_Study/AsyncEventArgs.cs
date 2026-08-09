using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using static System.Collections.Specialized.BitVector32;

namespace C__Server_Study
{
    public enum IoOperationType
    {
        Accept,
        Receive,
        Send
    }

    public class SocketAsyncEventArgsPool
    {
        private readonly Stack<SocketAsyncEventArgs> _pool;

        public SocketAsyncEventArgsPool(int capacity)
        {
            _pool = new Stack<SocketAsyncEventArgs>(capacity);
        }

        public void Push(SocketAsyncEventArgs item)
        {
            if (item == null)
                return;

            lock(_pool)
            {
                _pool.Push(item);
            }
        }

        public SocketAsyncEventArgs Pop()
        {
            lock (_pool)
            {
                if(_pool.Count > 0)
                {
                    return _pool.Pop();
                }
                else
                {
                    return null;
                }
            }
        }

        public int Count
        {
            get
            {
                lock (_pool)
                {
                    return (_pool.Count);
                }
            }
        }
    }

    public class GameServer
    {
        private readonly int _maxConnection;
        private readonly int _receiveBufferSize;
        private readonly RecvBuffer _recvBuffer;
        private readonly Queue<SendBuffer> _sendBuffer;

        private readonly SocketAsyncEventArgsPool _readPool;
        private readonly SocketAsyncEventArgsPool _writePool;
        private readonly Semaphore _maxConnectionsEnforcer;

        private readonly object _sendLock = new object();

        private Socket _listenSocket;

        private bool _sendRegistered = false;

        public GameServer(int maxConnection, int receiveBufferSize)
        {
            _maxConnection = maxConnection;
            _receiveBufferSize = receiveBufferSize;

            _recvBuffer = new RecvBuffer(receiveBufferSize);
            _sendBuffer = new Queue<SendBuffer>();
            
            _readPool = new SocketAsyncEventArgsPool(maxConnection);
            _writePool = new SocketAsyncEventArgsPool(maxConnection);

            // 최대 연결 수 제한을 위한 세마포어
            _maxConnectionsEnforcer = new Semaphore(maxConnection, maxConnection);
        }

        public void Initialize()
        {
            // SocketAsyncEventArgs 객체 풀 준비
            SocketAsyncEventArgs readArgs;

            for(int i = 0; i < _maxConnection; i++)
            {
                // 수신용 SocketAsyncEventArgs 준비.
                readArgs = new SocketAsyncEventArgs();
                readArgs.Completed += IO_Completed;
                readArgs.UserToken = new AsyncUserToken();

                readArgs.SetBuffer(_recvBuffer.WriteMemory);

                _readPool.Push(readArgs);
            }

            for(int i = 0; i < _maxConnection; i++)
            {
                SocketAsyncEventArgs writeArgs = new SocketAsyncEventArgs();
                writeArgs.Completed += IO_Completed;
                writeArgs.UserToken = new AsyncUserToken();

                _writePool.Push(writeArgs);
            }
        }

        public void Start(IPEndPoint localEndPoint)
        {
            // 서버 소켓 생성 및 바인딩
            _listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            _listenSocket.Bind(localEndPoint);

            // 최대 100개의 대기 연결 허용
            _listenSocket.Listen(100);

            // 연결 수락
            StartAccept(null);

            Console.WriteLine($"서버가 {localEndPoint}에서 시작됨");
        }

        private void StartAccept(SocketAsyncEventArgs acceptEventArg)
        {
            if(acceptEventArg == null)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.Completed += IO_Completed;
            }
            else
            {
                // 소켓 핸들 정리
                acceptEventArg.AcceptSocket = null;
            }

            // 새 연결을 받기 전에 세마포어 대기.
            _maxConnectionsEnforcer.WaitOne();

            bool willeRaiseEvent = _listenSocket.AcceptAsync(acceptEventArg);
            if(!willeRaiseEvent)
            {
                //동기적으로 완료된 경우
                ProcessAccept(acceptEventArg);
            }
        }

        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            if(e.SocketError == SocketError.Success)
            {
                // 풀에서 수신용 SocketAsyncEventArgs 가져오기.
                SocketAsyncEventArgs readEventArgs = _readPool.Pop();

                // 풀이 비어있으면 연결 거부
                if(readEventArgs == null)
                {
                    Console.WriteLine("서버가 최대 용량에 도달했습니다. 연결 거부됨.");
                    e.AcceptSocket.Close();
                }
                else
                {
                    // 새 소켓에 대한 참조 저장
                    AsyncUserToken token = (AsyncUserToken)readEventArgs.UserToken;
                    token.socket = e.AcceptSocket;

                    Console.WriteLine($"클라이언트 연결됨: {e.AcceptSocket.RemoteEndPoint}");

                    // 데이터 수신 시작
                    bool willRaiseEvent = e.AcceptSocket.ReceiveAsync(readEventArgs);
                    if(!willRaiseEvent)
                    {
                        ProcessReceive(readEventArgs);
                    }
                }

                StartAccept(e);
            }
        }

        private void IO_Completed(object? sender, SocketAsyncEventArgs e)
        {
            switch(e.LastOperation)
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
            AsyncUserToken token = (AsyncUserToken)e.UserToken;

            // 연결이 정상적으로 닫혔거나 오류가 발생한 경우
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                CloseClientSocket(e);
                return;
            }

            // 버퍼 데이터 채크
            if(_recvBuffer.OnWrite(e.BytesTransferred) == false)
            {
                CloseClientSocket(e);
                Console.WriteLine("Recv : 0 이여서 disconnect 함");
                return;
            }

            // 수신된 데이터 처리
            ReadOnlyMemory<byte> receivedData = _recvBuffer.ReadMemory.Slice(0, e.BytesTransferred);

            //임시 코드
            SendBuffer sendBuffer = new SendBuffer(e.BytesTransferred);
            sendBuffer.CopyData(receivedData);
            ////

            if (!_recvBuffer.OnRead(e.BytesTransferred))
            {
                sendBuffer.Dispose();
                CloseClientSocket(e);
                Console.WriteLine("RecvBuffer read failed.");
                return;
            }

            _recvBuffer.Clear();

            // 데이터 처리 로직 (여기에서는 에코 서버로 구현)
            Send(token.socket, sendBuffer);

            // 다시 수신 대기
            bool wileRaiseEvent = token.socket.ReceiveAsync(e);
            if(!wileRaiseEvent)
            {
                ProcessReceive(e);
            }
        }

        private void Send(Socket socket, SendBuffer data)
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
                SendResponse(socket);
            }
        }

        private void SendResponse(Socket socket)
        {
            // 풀에서 송신용 SocketAsyncEventArgs 가져오기
            SocketAsyncEventArgs writeEventArgs = _writePool.Pop();

            if(writeEventArgs == null)
            {
                lock (_sendLock)
                {
                    _sendRegistered = false;
                }

                // 풀이 고갈되었으면 다시 시도를 위해 큐에 넣을 수 있음
                // 여기서는 간단히 오류 로그만 남김
                Console.WriteLine("송신 풀이 고갈되었습니다.");
                return;
            }

            AsyncUserToken token = (AsyncUserToken)writeEventArgs.UserToken;
            token.socket = socket;
            token.sendBuffer.Clear();

            {
                lock(_sendLock)
                {
                    while(_sendBuffer.Count > 0)
                    {
                        SendBuffer sendBuffer = _sendBuffer.Dequeue();

                        token.sendBuffer.Add(sendBuffer);
                    }
                }
            }

            List<ArraySegment<byte>> bufferList = new List<ArraySegment<byte>>();

            foreach (SendBuffer sendBuffer in token.sendBuffer)
            {
                bufferList.Add(sendBuffer.ToArraySegment());
            }

            writeEventArgs.BufferList = bufferList;

            // 비동기 전송 시작
            bool willRaiseEvent = socket.SendAsync(writeEventArgs);
            if (!willRaiseEvent)
            {
                ProcessSend(writeEventArgs);
            }
        }

        private void ProcessSend(SocketAsyncEventArgs e)
        {
            AsyncUserToken token = (AsyncUserToken)e.UserToken!;

            try
            {
                if (e.SocketError != SocketError.Success || e.BytesTransferred <= 0)
                {
                    CloseClientSocket(e);
                    return;
                }
            }
            finally
            {
                foreach (SendBuffer sendBuffer in token.sendBuffer)
                {
                    sendBuffer.Dispose();
                }

                token.sendBuffer.Clear();
                token.socket = null;

                e.BufferList = null;

                _writePool.Push(e);
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

            if (registerSend && token.socket != null)
            {
                SendResponse(token.socket);
            }
        }

        private void CloseClientSocket(SocketAsyncEventArgs e)
        {
            AsyncUserToken token = e.UserToken as AsyncUserToken;

            // 소켓 연결 종료
            try
            {
                token.socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception) { /* 이미 닫혀 있을 수 있음 */ }

            token.socket.Close();
            token.socket = null;

            // 연결 개수 제한 세마포어 증가
            _maxConnectionsEnforcer.Release();

            // SocketAsyncEventArgs를 풀로 반환
            _readPool.Push(e);
        }

        // 서버 중지
        public void Stop()
        {
            try
            {
                _listenSocket.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"서버 종료 중 오류: {ex.Message}");
            }
        }
    }

    public class AsyncUserToken()
    {
        public Socket socket { get; set; }
        public List<SendBuffer> sendBuffer { get; set; }
    }
}
