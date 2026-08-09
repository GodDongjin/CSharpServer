using C__Server_Study;
using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SimpleSocketServer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            GameServer gameServer = new GameServer("127.0.0.1", 8888, 100, TimeSpan.FromMinutes(5));

            Console.WriteLine("게임 서버 시작 중...");
            Console.WriteLine("종료하려면 'quit' 입력");

            var serverTask = gameServer.StartAsync();

            // 콘솔 입력 처리
            while (true)
            {
                var input = Console.ReadLine();
                if (input?.ToLower() == "quit")
                {
                    break;
                }
                else if (input?.ToLower() == "status")
                {
                    Console.WriteLine($"현재 연결 수: {gameServer.GetConnectionCount()}");
                }
            }

            Console.WriteLine("서버 종료 중...");
            gameServer.Stop();
            await serverTask;
            Console.WriteLine("서버가 종료되었습니다.");
        }

        public void StartServer(Socket listener)
        {
            try
            {
                IPAddress iPAddress = IPAddress.Parse("127.0.0.1");
                int port = 8888;

                listener.Bind(new IPEndPoint(iPAddress, port));
                listener.Listen(10);

                Console.WriteLine("서버가 시작되었습니다. 클라이언트 연결 대기 중.....");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        

    public void SimpleServer()
    {
        IPAddress iPAddress = IPAddress.Parse("127.0.0.1");
        int port = 8888;

        // listen 소캣 생성
        Socket listener = new Socket(iPAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

        try
        {
            // listen 소캣 ip, port 설정.
            listener.Bind(new IPEndPoint(iPAddress, port));

            // 연결 대기 상태로 설정 (최대 10개 연결 대기열)
            listener.Listen(10);

            Console.WriteLine("서버가 시작되었습니다. 클라이언트 연결 대기 중.....");

            while (true)
            {
                // 클라이언트 연결 요청 수락
                Socket handler = listener.Accept();

                // 클라이언트 데이터 수신
                byte[] buffer = new byte[1024];
                int received = handler.Receive(buffer);
                string data = Encoding.UTF8.GetString(buffer, 0, received);

                Console.WriteLine($"클라이언트 수신 : {data}");

                // 클라이언트에 데이터 송신
                string response = "안녕하세요, 클라이언트!";
                byte[] responseBuffer = Encoding.UTF8.GetBytes(response);
                handler.Send(responseBuffer);

                // 클라이언트 소켓 연결 해제
                handler.Shutdown(SocketShutdown.Both);
                handler.Close();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }

        static public void EchoServer()
    {
        IPAddress ipAddress = IPAddress.Parse("127.0.0.1");
        int port = 8888;

        Socket listener = new Socket(ipAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

        try
        {
            listener.Bind(new IPEndPoint(ipAddress, port));
            listener.Listen(10);

            Console.WriteLine("서버 시작. 클라이언트 연결 대기 중....");

            while (true)
            {
                Socket handler = listener.Accept();

                if (!handler.Connected)
                {
                    continue;
                }

                while (true)
                {
                    byte[] buffer = new byte[1024];
                    int received = handler.Receive(buffer);
                    if (received <= 0)
                    {
                        continue;
                    }
                    string data = Encoding.UTF8.GetString(buffer, 0, received);

                    Console.WriteLine($"클라이언트 : {data}");

                    string response = "안녕하세요, 클라이언트!";
                    byte[] responseBuffer = Encoding.UTF8.GetBytes(response);
                    handler.Send(responseBuffer);

                    Task.Delay(1000).Wait();
                }

            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }
    }


    class Server
    {
        List<ClientHandler> clientList = new List<ClientHandler>();

        private readonly IPAddress ipAddress;
        private readonly int port;
        private readonly Socket listener;

        private static readonly object clientLock = new();

        public Server()
        {
            ipAddress = IPAddress.Parse("127.0.0.1");
            port = 8080;
            listener = new Socket(ipAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        }

       public void StartServer()
       {
            try
            {
                listener.Bind(new IPEndPoint(ipAddress, port));
                listener.Listen(10);

                Console.WriteLine($"채팅 서버가 시작되었습니다. ({ipAddress}:{port})");

                while (true)
                {
                    Console.WriteLine("클라이언트 연결 대기 중...");

                    Socket socket = listener.Accept();
                    ClientHandler client = new ClientHandler(socket);

                    lock (clientLock)
                    {
                        clientList.Add(client);
                    }

                    Thread clientThread = new(client.ClientReceive);
                    clientThread.IsBackground = true;
                    clientThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                listener.Close();
            }
        }

        public void BroadcastMessage(string message, ClientHandler? sender = null)
        {
            foreach(ClientHandler client in clientList)
            {
                if(client == sender)
                {
                    continue;
                }

                client.SendClient(message);
            }
        }
    }

    class ClientHandler
    {
        private readonly Socket clientSocket;
        private string nick;
        private readonly byte[] buffer = new byte[1024];

        public ClientHandler(Socket socket)
        {
            clientSocket = socket;
            nick = "";
        }

        public void ClientReceive()
        {
            try
            {
                while (true)
                {
                    int byteRead = clientSocket.Receive(buffer);
                    string message = Encoding.UTF8.GetString(buffer, 0, byteRead);

                    if (message.StartsWith("/nick "))
                    {
                        string oldNick = nick;
                        string newnick = message.Substring(6).Trim();

                        if (oldNick == newnick)
                        {
                            return;
                        }

                        nick = newnick;
                    }
                    else if (message.StartsWith("/chat "))
                    {
                        string chat = message.Substring(6).Trim();

                        message = (nick + " : " + chat);
                        //server.BroadcastMessage(message);
                    }
                }
            }
            catch(SocketException ex)
            {
                Console.WriteLine(ex);
            }
        }

        public void SendClient(string message)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(message);
            clientSocket.Send(buffer);
        }

        public void Disconnect()
        {
            try
            {
                clientSocket.Shutdown(SocketShutdown.Both);
            }
            catch (SocketException)
            {
                // 이미 연결이 끊겼을 경우 무시
            }
            finally
            {
                clientSocket.Close();
            }

            Console.WriteLine($"클라이언트 연결 종료: {nick}");
        }
    }
}