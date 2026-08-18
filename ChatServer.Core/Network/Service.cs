using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

using ChatServer.Core.Interface;
using ChatServer.Core.Packet;

namespace ChatServer.Core.Network
{
    public enum SERVICE_TYPE
    {
        NONE,
        CHAT_SERVER,
        LOGIN_SERVER,
    }

    public class Service : ISessionOwner
    {
        private readonly SessionManager? _sessionManager;
        private readonly PacketHandler? _packetHandler;

        private Listen? _listen;

        private readonly SocketAsyncEventArgsPool? _readPool;
        private readonly SocketAsyncEventArgsPool? _writePool;

        private readonly Int32 _maxConnection;
        private readonly SERVICE_TYPE _serviceType;

        public SessionManager? SessionManager { get { return _sessionManager; } }
        public PacketHandler? PacketHandler { get { return _packetHandler; } }
        public SocketAsyncEventArgsPool? ReadPool { get { return _readPool; } }
        public SocketAsyncEventArgsPool? WritePool { get { return _writePool; } }
        public Listen? Listen { get { return _listen; }  set { _listen = value; } }

        public Service(SERVICE_TYPE serviceType, SessionManager sessionManager, PacketHandler packetHandler, Int32 maxConnection)
        {
            _readPool = new SocketAsyncEventArgsPool(maxConnection);
            _writePool = new SocketAsyncEventArgsPool(maxConnection);

            for (int i = 0; i < maxConnection; i++)
            {
                _readPool.Push(new SocketAsyncEventArgs());
                _writePool.Push(new SocketAsyncEventArgs());
            }

            _serviceType = serviceType;

            _packetHandler = packetHandler;
            _sessionManager = sessionManager;
            _maxConnection = maxConnection;
        }

        public void StartServer(IPAddress ipAddress, Int16 port)
        {
            IPEndPoint iPEndPoint = new IPEndPoint(ipAddress, port);
            _listen = new Listen(iPEndPoint, this);

            _listen.StartListen(); 
        }

        public bool StopServer(TimeSpan timeout)
        {
            Listen.StopAsyncListen();

            if (!_sessionManager.AllReleaseSession(timeout))
                return false;

            _readPool.Dispose();
            _writePool.Dispose();
            return true;
        }

        public void ReleaseSession(Session session)
        {
            if (!_sessionManager.ReleaseSession(session))
                return;

            _readPool.Push(session.ReadArgs);
            _writePool.Push(session.WriteArgs);
        }
    }
}


