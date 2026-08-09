using SimpleSocketServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace C__Server_Study
{
    class ChatServer
    {
        private static readonly List<ClientHandler> clientList = new List<ClientHandler>();
        private static readonly object clientLock = new();

        public void StartChatServer()
        {
            Socket serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            try
            {
                IPAddress iPAddress = IPAddress.Parse("127.0.0.1");
                int port = 8888;
                serverSocket.Bind(new IPEndPoint(iPAddress, port));
                serverSocket.Listen(10);

                Console.WriteLine($"채팅 서버가 시작되었습니다. ({iPAddress}:{port})");

                while (true) 
                {
                    Console.WriteLine("클라이언트 연결 대기중...");

                    //클라이언트 연결 수락
                    Socket client = serverSocket.Accept();

                    ClientHandler handler = new ClientHandler(client);
                    lock (clientList)
                    {
                        clientList.Add(handler);
                    }

                    Thread clientThread = new(handler.StartClient);
                    clientThread.IsBackground = true;
                    clientThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"서버 오류: {ex.Message}");
            }
            finally
            {
                // 서버 소켓 닫기
                serverSocket.Close();
            }
        }

        public static void BroadcastMessage(string message, ClientHandler? sender = null)
        {
            List<ClientHandler> list;

            lock (clientLock)
            {
                list = clientList;
            }

            foreach(ClientHandler client in list)
            {
                if(sender == client){
                    continue;
                }

                try
                {
                    client.SendMessage(message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"메시지 전송 오류: {ex.Message}");
                }
            }
        }

        public static void RemoveClient(ClientHandler client)
        {
            lock (clientLock)
            {
                if(clientList.Contains(client))
                {
                    clientList.Remove(client);
                    Console.WriteLine($"클라이언트가 제거되었습니다. 현재 연결 수: {clientList.Count}");
                }
                
            }
        }
    }

    public class ClientHandler
    {
        private readonly Socket clientSocket;
        private string nickname = "Guest";
        private bool isConnected = true;

        public ClientHandler(Socket socket)
        {
            clientSocket = socket;

            // 클라이언트 정보 출력
            IPEndPoint? remoteEndPoint = clientSocket.RemoteEndPoint as IPEndPoint;
            Console.WriteLine($"클라이언트 연결됨: {remoteEndPoint?.Address}:{remoteEndPoint?.Port}");
        }

        public void StartClient()
        {
            try
            {
                // 입잡 메시지 전송
                SendMessage("채팅 서버에 연결되었습니다. 닉네임을 입력하세요: /nick <닉네임>");

                // 클라이언트로부터 데이터 수신 대기
                byte[] buffer = new byte[1024];

                while (true)
                {
                    try
                    {
                        int bytesRead = clientSocket.Receive(buffer);

                        if (bytesRead == 0)
                        {
                            // 클라이언트 연결 종료.
                            CloseConnection();
                            ChatServer.RemoveClient(this);
                            break;
                        }

                        // 수신된 메시지 처리
                        string message = Encoding.UTF8.GetString(buffer);
                        ProcessMessage(message);
                    }
                    catch (SocketException)
                    {
                        // 소켓 오류 시 연결 종료.
                        CloseConnection();
                        ChatServer.RemoveClient(this);
                        break;
                    }

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"클라이언트 처리 오류: {ex.Message}");
            }
            finally
            {
                // 연결 종료 처리
                isConnected = false;
                CloseConnection();

                // 퇴장 메시지 브로드캐스팅
                ChatServer.BroadcastMessage($"[시스템] {nickname}님이 퇴장했습니다.");

                // 클라이언트 목록에서 제거
                ChatServer.RemoveClient(this);
            }
        }

        private void ProcessMessage(string message)
        {
            if (message.StartsWith("/nick"))
            {
                string newNickname = message.Substring(6).Trim();

                if (!string.IsNullOrEmpty(newNickname))
                {
                    string oldNickname = nickname;
                    nickname = newNickname;

                    // 변경 사실 알림
                    SendMessage($"닉네임이 {newNickname}(으)로 변경되었습니다.");

                    // 다른 사용자에게 알림
                    ChatServer.BroadcastMessage($"[시스템] {oldNickname}님이 {newNickname}(으)로 닉네임을 변경했습니다.", this);
                }
                else
                {
                    SendMessage("올바르지 않은 닉네임입니다.");
                }
            }
            else
            {
                // 메시지 브로드캐스팅
                ChatServer.BroadcastMessage($"[{nickname}] {message}");

                // 콘솔에 출력
                Console.WriteLine($"[{nickname}] {message}");
            }
        }

        public void SendMessage(string message)
        {
            if (isConnected)
            {
                byte[] data = Encoding.UTF8.GetBytes(message);
                clientSocket.Send(data);
            }
        }

        public void CloseConnection()
        {
            if (clientSocket != null)
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

                Console.WriteLine($"클라이언트 연결 종료: {nickname}");
            }
        }
    }
}
