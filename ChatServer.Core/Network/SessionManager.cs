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
        private readonly ConcurrentDictionary<Int64, Session> _sessionDic = new ConcurrentDictionary<Int64, Session>();

        private readonly Func<Session> _sessionFactory;

        private readonly Int32 _maxSessionCount;

        private readonly SemaphoreSlim _maxSessionsEnforcer;
        private readonly ManualResetEventSlim _allSessionsReleased = new(initialState: true);


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
            if (!_maxSessionsEnforcer.Wait(0))
            {
                return null;
            }

            Session? session = null;

            try
            {
                if (!_sessionQueue.TryDequeue(out session))
                {
                    throw new InvalidOperationException("사용 가능한 세션이 없습니다.");
                }

                session.Initialize(sessionOwner, packetHandler, socket, readArgs, writeArgs);

                _allSessionsReleased.Reset();

                if (!_sessionDic.TryAdd(session._sessionID, session))
                {
                    session.Disconnect();
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

                if (_sessionDic.IsEmpty)
                    _allSessionsReleased.Set();

                socket.Close();
                _maxSessionsEnforcer.Release();

                return null;
            }
        }

        public bool ReleaseSession(Session session)
        {
            if (!_sessionDic.TryRemove(session._sessionID, out _))
                return false;

            session.Reset();
            _sessionQueue.Enqueue(session);
            _maxSessionsEnforcer.Release();

            if (_sessionDic.IsEmpty)
                _allSessionsReleased.Set();

            return true;
        }

        public bool AllReleaseSession(TimeSpan timeout)
        {
            Session[] sessions = _sessionDic.Values.ToArray();

            if (sessions.Length == 0)
                return true;

            foreach (Session session in sessions)
                session.Disconnect();

            return _allSessionsReleased.Wait(timeout);
        }
    }
}


