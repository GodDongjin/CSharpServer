using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.Core.Network
{
    public class SocketUtile
    {
        // address 재사용 옵션
        public void SocketReuse(Socket? socket, bool flag = false)
        {
            // ArgumentNullException : 오브젝트 null채크 및 null이면 Throw를 발생시켜 오류를 알려줌.
            ArgumentNullException.ThrowIfNull(socket);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        }

        // 수신 버퍼 사이즈 수정 옵션
        public void SocketReceiveBuffer(Socket? socket, Int32 byteSize = 65536)
        {
            ArgumentNullException.ThrowIfNull(socket);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, byteSize);
        }

        // 송신 버퍼 사이즈 수정 옵션
        public void SocketSendBuffer(Socket? socket, Int32 byteSize = 65536)
        {
            ArgumentNullException.ThrowIfNull(socket);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, byteSize);
        }

        // Nagle 알고리즘 사용 여부 옵션
        public void SocketNoDelay(Socket? socket, bool flag = false)
        {
            ArgumentNullException.ThrowIfNull(socket);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.NoDelay, flag);
        }

        // 타임아웃 설정 옵션
        public void SocketTimeOut(Socket? socket, Int32 time = 10000)
        {
            ArgumentNullException.ThrowIfNull(socket);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, time);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, time);
        }

        // 연결 유지를 위한 주기적인 패킷 전송 여부 옵션
        public void SocketKeepAlive(Socket? socket, bool flag = false)
        {
            ArgumentNullException.ThrowIfNull(socket);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, flag);
        }

        // Broadcast 허용 여부 (UDP 전용)
        public void UDPSocketBroadcast(Socket? socket, bool flag = false)
        {
            ArgumentNullException.ThrowIfNull(socket);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Broadcast, flag);
        }

        // 패킷이 네트워크에서 살아있을 수 있는 최대 홉(hop) 수 설정
        public void SocketTTL(Socket? socket, Int32 count = 64)
        {
            ArgumentNullException.ThrowIfNull(socket);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.IpTimeToLive, count);
        }
    }
}
