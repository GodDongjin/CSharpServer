using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace C__Server_Study
{
    public class AsyncChatServer
    {

        // 비동기 작업자에게 취소 신호를 전달해주는 관리자 객체이다.
        // token을 작업자에게 전달해주고 작업자는 token을 채크하면서 취소 신호가 생겼는지 확인해야 한다. 이후 cts에서 취소 신호를 tokem을 가지고 있는 작업자에서 신호를 보냄
        private readonly CancellationTokenSource _cts; 

        private readonly TcpListener _listener;
        private readonly ConcurrentDictionary<string, ChatClient> _clientList;
        private readonly ConcurrentDictionary<string, Room> _roomList;
        private readonly SemaphoreSlim _maxClientsLock;

        public AsyncChatServer(string ip, Int32 port, Int32 maxClient)
        {
            IPAddress iPAddress = IPAddress.Parse(ip);
            _listener = new TcpListener(iPAddress, port);
            _cts = new CancellationTokenSource();
            _clientList = new ConcurrentDictionary<string, ChatClient>();
            _roomList = new ConcurrentDictionary<string, Room>();
            _maxClientsLock = new SemaphoreSlim(maxClient);
        }

        public async Task StartAsync()
        {
            _listener.Start();
            Console.WriteLine($"서버가 {_listener.LocalEndpoint}에서 시작됨");

            try
            {
                // Token.IsCancellationRequested : 토큰의 취소 요청이 왔는지 표시되는 bool값.
                while (!_cts.Token.IsCancellationRequested)
                {
                    // SemaphoreSlim.WaitAsync() : 세마포어 점유가능한 개수가 0이면 들어갈 수 있을 때까지 비동기로 기다리는 메서드
                    await _maxClientsLock.WaitAsync(_cts.Token);
                    _ = AcceptClientAsync();
                }
            }
            catch (OperationCanceledException) // OperationCanceledException : Token.IsCancellationRequested가 true상태가 되면 호출되는 이벤트.
            {
                // 정상 종료
            }
            catch (Exception ex)
            {
                Console.WriteLine($"서버 오류 : {ex.Message}");
            }
            finally
            {
                _listener.Stop();
                Console.WriteLine("서버가 종료됨");
            }
        }

        private async Task AcceptClientAsync()
        {
            TcpClient tcpClient = null;

            try
            {
                // Accept가 발생할 때 까지 대기.
                tcpClient = await _listener.AcceptTcpClientAsync();
                string clientId = Guid.NewGuid().ToString();    // Guid : 고유번호 즉 UID를 생성해주는 객체.

                ChatClient client = new ChatClient(clientId, tcpClient);

                if (_clientList.TryAdd(clientId, client))
                {
                    Console.WriteLine($"클라이언트 연결 : {clientId} (현재 접속자 : {_clientList.Count})");

                    // 환영 메시지 전송
                    await client.SendMessageAsync($"환영합니다! 당신의 ID : {client.Id}");

                    // 다른 사용자들에게 입장 알림
                    await BroadcastMessageAsync($"사용자가 입장했습니다! : {clientId}", clientId);

                    // _ : task에 반환된 값을 변수에 저장하지 않겠다라는 뜻.
                    // ContinueWith : HandleClientAsync(client) 메서드가 끝난뒤 실행될 후속 작업.
                    _ = HandleClientAsync(client).ContinueWith(task =>
                    {
                        if (task.IsFaulted)
                        {
                            Console.WriteLine($"클라이언트 처리 오류 : {task.Exception?.InnerException?.Message}");
                        }

                        if (_clientList.TryRemove(clientId, out _))
                        {
                            Console.WriteLine($"클라이언트 연결 종료 : {clientId} (현재 접속자 : {_clientList.Count})");
                            BroadcastMessageAsync($"사용자가 퇴장했습니다 : {clientId}", null).Wait();
                        }

                        client.Dispose();

                        _maxClientsLock.Release();
                    });
                }
                else
                {
                    // 클라이언트 추가 실패
                    tcpClient.Dispose();
                    _maxClientsLock.Release();
                }
            }
            catch (SocketException ex)
            {
                Console.WriteLine($"클라이언트 접속 처리 오류: {ex.Message}");
                tcpClient?.Dispose();
                _maxClientsLock.Release();
            }
        }

        private async Task HandleClientAsync(ChatClient client)
        {
            try
            {
                while(!_cts.IsCancellationRequested)
                {
                    string message = await client.ReceiveMessageAsync();

                    if(string.IsNullOrEmpty(message))
                    {
                        // 빈 메시지는 연결 종료를 의미.
                        break;
                    }

                    if (message.StartsWith("/join"))
                    {
                       string room_name = message.Substring(6).Trim();

                        await RoomJoinAsync(room_name, client.Id, client);
                        client._roomId = room_name;
                        continue;
                    }

                    if(message.StartsWith("/leave"))
                    {
                        await RoomLeaveAsync(client._roomId, client.Id);
                        continue;
                    }

                    Console.WriteLine($"메시지 수신: [{client.Id}] {message}");

                    await BroadcastMessageAsync(message, client.Id);
                }
            }
            catch(Exception)
            {

            }
        }

        private async Task BroadcastMessageAsync(string message, string senderId)
        {
            var tasks = new List<Task>();

            foreach (var client in _clientList.Values)
            {
                if(client.Id == senderId)
                {
                    continue;
                }

                tasks.Add(client.SendMessageAsync(message));
            }

            await Task.WhenAll(tasks);
        }

        private async Task RoomJoinAsync(string roomName, string user_id, ChatClient client)
        {
            try
            {
                Room room = _roomList.GetOrAdd(roomName, name => new Room(name, 10));

                await room.RoomJoinAsync(user_id, client);
                client._roomId = roomName;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"방 입장 오류 : {ex.Message}");
            }
        }

        private async Task RoomLeaveAsync(string roomName, string user_id)
        {
            if (_roomList.TryGetValue(roomName, out var room))
            {
                await room.RoomLeaveAsync(user_id);
            }
        }

        public void Stop()
        {
            _cts.Cancel();
        }
    }

    // IDisposable : 메모리 해제를 GC에 맏기는것이 아닌 직접 메모리 해제를 하겠다는 선언 객체.
    //               IDisposable를 사용할려면 필드안에 Dispose()를 구현해야 한다.
    public class ChatClient : IDisposable
    {
        private readonly TcpClient _tcpClient;
        private readonly NetworkStream _stream;     //소켓 통신을 위한 스트림 객체. 
        private readonly SemaphoreSlim _sendLock = new SemaphoreSlim(1, 1);

        public string Id { get; }
        public string _roomId { get; set; }

        public ChatClient(string id, TcpClient tcpClient)
        {
            Id = id;
            _roomId = "";
            _tcpClient = tcpClient;
            _stream = tcpClient.GetStream();
        }

        public async Task<string> ReceiveMessageAsync()
        {
            byte[] buffer = new byte[4096];
            int bytesRead = await _stream.ReadAsync(buffer, 0, buffer.Length);

            if(bytesRead == 0)
            {
                return null; // 연결 종료
            }

            return Encoding.UTF8.GetString(buffer, 0, bytesRead);
        }

        public async Task SendMessageAsync(string message)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(message);

            await _sendLock.WaitAsync();

            try
            {
                await _stream.WriteAsync(buffer, 0, buffer.Length);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        public void Dispose()
        {
            _stream?.Dispose();
            _tcpClient?.Dispose();
        }
    }

    public class Room
    {
        private readonly ConcurrentDictionary<string, ChatClient> _userList;
        private readonly string _roomName;
        private readonly Int32 _roomMaxCount;

        private readonly SemaphoreSlim _sendLock = new SemaphoreSlim(1, 1);

        public Room(string roomName, Int32 roomMaxCount)
        {
            _userList = new ConcurrentDictionary<string, ChatClient>();
            _roomName = roomName;
            _roomMaxCount = roomMaxCount;
        }

        public async Task RoomJoinAsync(string user_id, ChatClient client)
        {
            await _sendLock.WaitAsync();

            try
            {
                if (_userList.ContainsKey(user_id))
                {
                    await client.SendMessageAsync($"이미 입장한 방 입니다.");
                    return;
                }
                else if (_userList.Count >= _roomMaxCount)
                {
                    await client.SendMessageAsync($"이미 방이 꽉찼습니다.");
                    return;
                }

                if (_userList.TryAdd(user_id, client))
                {
                    await client.SendMessageAsync($"{_roomName} 방에 입장했습니다");

                    await RoomBroadcastMessageAsync($"{user_id}님이 {_roomName} 방에 입장했습니다.", user_id);
                }
            }
            finally
            {
                _sendLock.Release();
            }
        }

        public async Task RoomLeaveAsync(string user_id)
        {
            await _sendLock.WaitAsync();

            try
            {
                if (_userList.TryRemove(user_id, out var user))
                {
                    await user.SendMessageAsync($"{_roomName} 방에서 퇴장했습니다.");

                    await RoomBroadcastMessageAsync($"{user_id}님이 {_roomName}방에서 퇴장하였습니다.", user_id);
                }
            }
            finally
            {
                _sendLock.Release();
            }
        }

        private async Task RoomBroadcastMessageAsync(string message, string senderId)
        {
            var tasks = new List<Task>();

            foreach (var client in _userList.Values)
            {
                if (client.Id == senderId)
                {
                    continue;
                }

                tasks.Add(client.SendMessageAsync(message));
            }

            await Task.WhenAll(tasks);
        }
    }
}
