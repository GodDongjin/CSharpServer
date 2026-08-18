using ChatServer.Core.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.Core.Network
{
    public sealed class Listen
    {
        private Socket? _listenSocket;

        //readonly = 읽기만 가능한 변수라고 선언하는 키워드.
        private readonly IPEndPoint _iPEndPoint;

        private readonly Service _serivce;
        private readonly SocketUtile _socketUtile = new SocketUtile();

        private object _lock = new object();
        private bool _isStop;

        public Listen(IPEndPoint iPEndPoint, Service service)
        {
            _iPEndPoint = iPEndPoint;
            _serivce = service;
        }

        public void StartListen()
        {
            _isStop = false;

            _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listenSocket.Bind(_iPEndPoint);
            _listenSocket.Listen(backlog: 100);

            StartAsyncListen(null);

            Console.WriteLine($"{_serivce.ToString()} 서버가 {_iPEndPoint}에서 시작됨");
        }

        public void StartAsyncListen(SocketAsyncEventArgs? acceptEventArg)
        {
            if (_isStop)
                return;

            bool pending;

            lock(_lock)
            {
                if (_isStop || _listenSocket == null)
                    return;

                if (acceptEventArg == null)
                {
                    acceptEventArg = new SocketAsyncEventArgs();
                    acceptEventArg.Completed += OnAcceptCompleted;
                }
                else
                {
                    // 소켓 핸들 정리
                    acceptEventArg.AcceptSocket = null;
                }

                pending = _listenSocket!.AcceptAsync(acceptEventArg);
                
            }

            // 소켓 핸들에 값이 있는지 채크.
            if (!pending)
            {
                // 즉시 완료된 경우
                ProcessAccept(acceptEventArg);
            }
        }

        private void OnAcceptCompleted(object? sender, SocketAsyncEventArgs e)
        {
            ProcessAccept(e);
        }

        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            ArgumentNullException.ThrowIfNull(e);

            lock (_lock)
            {
                if (_isStop)
                {
                    e.AcceptSocket?.Close();
                    return;
                }
            }

            if (e.SocketError == SocketError.Success)
            {
                // 소켓 옵션 적용 (Nagle 비활성화로 지연시간 감소, KeepAlive로 유령 연결 방지)
                _socketUtile.SocketNoDelay(e.AcceptSocket, true);
                _socketUtile.SocketKeepAlive(e.AcceptSocket, true);

                // 풀에서 수신용 SocketAsyncEventArgs 가져오기
                SocketAsyncEventArgs readEventArgs = _serivce.ReadPool.Pop();
                SocketAsyncEventArgs writeEventArgs = _serivce.WritePool.Pop();

                // 풀이 비어있으면 연결 거부
                if(readEventArgs == null || writeEventArgs == null)
                {
                    if (readEventArgs != null)
                        _serivce.ReadPool.Push(readEventArgs);

                    if (writeEventArgs != null)
                        _serivce.WritePool.Push(writeEventArgs);

                    Console.WriteLine("서버가 최대 용량에 도달했습니다. 연결 거부됨.");
                    
                    e.AcceptSocket?.Close();
                    StartAsyncListen(e);

                    return;
                }
                else
                {
                    if(_serivce is null)
                    {
                        throw new InvalidOperationException($"{nameof(_serivce)}가 설정되지 않았습니다.");
                    }

                    Session session = _serivce.SessionManager.CreateSession(e.AcceptSocket, readEventArgs, writeEventArgs, _serivce as ISessionOwner, _serivce.PacketHandler as IPacketHandler);

                    if(session == null)
                    {
                        Console.WriteLine("Session 생성 실패. 연결 거부됨");
                        if (readEventArgs != null)
                            _serivce.ReadPool.Push(readEventArgs);

                        if (writeEventArgs != null)
                            _serivce.WritePool.Push(writeEventArgs);

                        e.AcceptSocket?.Close();
                        StartAsyncListen(e);
                        return;
                    }

                    Console.WriteLine($"{session._sessionID} 유저 생성 완료!");

                    session.StartSession();
                }
            }

            StartAsyncListen(e);
        }

        public void StopAsyncListen()
        {
            Socket? socketToClose;

            lock (_lock)
            {
                if (_isStop)
                    return;

                _isStop = true;

                socketToClose = _listenSocket;
                _listenSocket = null;
            }

            // Close는 콜백을 발생시킬 수 있으므로 lock 밖에서 수행
            socketToClose?.Close();

            Console.WriteLine("서버 정상 종료");
        }
    }
}
 
