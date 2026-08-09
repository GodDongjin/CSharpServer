// See https://aka.ms/new-console-template for more information

using ChatServer.Core.Network;
using ChatServer.App.Packet;
using System.Net;
using ChatServer.App.ChatSession;

namespace ChatServer.App
{
    class Program
    {
        static void Main(string[] args)
        {
            SessionManager sessionManager = new SessionManager(maxSessionCount: 1000, () => new GameSession());
            GSPacketHandler packetHandler = new GSPacketHandler();

            packetHandler.Initialize();

            Service chatService = new Service(SERVICE_TYPE.CHAT_SERVER, sessionManager, packetHandler, maxConnection: 1000);

            IPAddress iPAddress = IPAddress.Parse("127.0.0.1");

            chatService.StartServer(iPAddress, 7777);


            while (true)
            {
                var input = Console.ReadLine();
                if (input?.ToLower() == "quit")
                {
                    break;
                }
                else if (input?.ToLower() == "status")
                {
                    //Console.WriteLine($"현재 연결 수: {chatService.GetConnectionCount()}");
                }
            }

            Console.WriteLine("서버 종료 중...");
            chatService.StopServer();
            //await serverTask;
            Console.WriteLine("서버가 종료되었습니다.");
        }
    }
    
}