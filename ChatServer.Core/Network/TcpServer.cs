using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.Core.Network
{
    // sealed : 확장/상속 금지 키워드 - 구조를 고정해서 쓰겠다라는 의미
    public sealed class TcpServer
    {
        private readonly IPEndPoint _endPoint;
        //private readonly Sessio
    
        private Socket? _listenSocket;

        public TcpServer(IPEndPoint endPoint)
        {
            _endPoint = endPoint;
        }
    }
}
