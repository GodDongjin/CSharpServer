using ChatServer.Core.Interface;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using static System.Collections.Specialized.BitVector32;

namespace ChatServer.Core.Network
{
    public class SessionManager
    {
        private readonly ConcurrentQueue<Session> _sessionQueue = new ConcurrentQueue<Session>();
        private readonly ConcurrentDictionary<Guid, Session> _sessionDic = new ConcurrentDictionary<Guid, Session>();

        private readonly Func<Session> _sessionFactory;

        private readonly Int32 _maxSessionCount;

        private readonly SemaphoreSlim _maxSessionsEnforcer;


        public SessionManager(Int32 maxSessionCount, Func<Session> sessionFactory) 
        {
            _maxSessionCount = maxSessionCount;
            _maxSessionsEnforcer = new SemaphoreSlim(maxSessionCount, maxSessionCount);

            _sessionFactory = sessionFactory;

            for (int i = 0; i < _maxSessionCount; i++)
            {
                _sessionQueue.Enqueue(_sessionFactory());
            }
        }

        public Session? CreateSession(Socket socket, SocketAsyncEventArgs readArgs, SocketAsyncEventArgs writeArgs, ISessionOwner sessionOwner, IPacketHandler packetHandler)
        {
            _maxSessionsEnforcer.Wait();

            Session? session = null;

            try
            {
                if (!_sessionQueue.TryDequeue(out session))
                {
                    throw new InvalidOperationException("사용 가능한 세션이 없습니다.");
                }

                session.Initialize(sessionOwner, packetHandler, socket, readArgs, writeArgs);

                if (!_sessionDic.TryAdd(session._id, session))
                {
                    throw new InvalidOperationException("세션 등록 중 오류 발생");
                }

                return session;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

                if (session != null)
                {
                    session.Reset();
                    _sessionQueue.Enqueue(session);
                }

                socket.Close();
                _maxSessionsEnforcer.Release();

                return null;
            }
        }

        public bool ReleaseSession(Session session)
        {
            if (!_sessionDic.TryRemove(session._id, out _))
                return false;

            session.Reset();
            _sessionQueue.Enqueue(session);
            _maxSessionsEnforcer.Release();
            return true;
        }
    }
}
