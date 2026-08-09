using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace C__Server_Study
{
    public class ApmModel
    {
        public void StartConnect()
        {
            IPAddress iPAddress = IPAddress.Parse("127.0.0.1");
            Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Bind(new IPEndPoint(iPAddress, 8888));
            socket.Listen(10);
            Console.WriteLine("연결 대기 중...");

            while(true)
            {
                socket.BeginAccept(ConnectCallback, socket);
            }
            
        }

        public void ConnectCallback(IAsyncResult ar)
        {
            try
            {
                Socket socket = (Socket)ar.AsyncState;
                socket.EndAccept(ar);
                Console.WriteLine("연결 성공");

                byte[] buffer = new byte[1024];
                socket.BeginReceive(buffer, 0, buffer.Length, SocketFlags.None, RecevieCallback,
                    new StateObject { Socket = socket, Buffer = buffer });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"연결 실패 : {ex.Message}");
            }
        }

        public void RecevieCallback(IAsyncResult ar)
        {
            
        }

        private class StateObject
        {
            public Socket Socket { get; set; }
            public byte[] Buffer { get; set; }
        }
    }
}
