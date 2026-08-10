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
        readonly SocketUtile _socketUtile = new SocketUtile();

        //readonly = 읽기만 가능한 변수라고 선언하는 키워드.
        private readonly IPEndPoint _iPEndPoint;

        private readonly Service _serivce;

        public Listen(IPEndPoint iPEndPoint, Service service)
        {
            _iPEndPoint = iPEndPoint;
            _serivce = service;
        }

        public void StartListen()
        {
            _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listenSocket.Bind(_iPEndPoint);
            _listenSocket.Listen(backlog: 100);

            StartAsyncListen(null);

            Console.WriteLine($"{_serivce.ToString()} 서버가 {_iPEndPoint}에서 시작됨");
        }

        public void StartAsyncListen(SocketAsyncEventArgs acceptEventArg)
        {
            // 소켓 핸들에 값이 있는지 채크.
            if(acceptEventArg == null)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.Completed += OnAcceptCompleted;
            }
            else
            {
                // 소켓 핸들 정리
                acceptEventArg.AcceptSocket = null;
            }

            bool pending =  _listenSocket!.AcceptAsync(acceptEventArg);
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
            if(e.SocketError == SocketError.Success)
            {
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
                    // 소켓 옵션 설정.
                    _socketUtile.SocketReuse(e.AcceptSocket, false);
                    _socketUtile.SocketNoDelay(e.AcceptSocket, true);
                    _socketUtile.SocketKeepAlive(e.AcceptSocket, true);

                    // 소켓 생성.
                    Session session = _serivce.SessionManager.CreateSession(e.AcceptSocket, readEventArgs, writeEventArgs, _serivce as ISessionOwner, _serivce.PacketHandler as IPacketHandler);

                    if(session == null)
                    {
                        Console.WriteLine("Session 생성 실패. 연결 거부됨");
                        e.AcceptSocket?.Close();
                        StartAsyncListen(e);
                        return;
                    }

                    Console.WriteLine($"{session._id} 유저 생성 완료!");

                    session.StartSession();
                }
            }

            StartAsyncListen(e);
        }

        public void StopAsyncListen()
        {
            _listenSocket?.Close();

            Console.WriteLine("서버 정상 종료");
        }
    }
}
 