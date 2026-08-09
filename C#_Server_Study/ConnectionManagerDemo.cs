using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Quic;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace C__Server_Study
{
    /*public class GameServer
    {
        private readonly TcpListener _listener;
        private readonly ConnectionManager _connectionManager;
        private readonly CancellationTokenSource _cts;
        private bool _isRunning;

        public GameServer(string ipAddress, int port, int maxConnection, TimeSpan connectionTimeout)
        {
           IPAddress ip = IPAddress.Parse(ipAddress);
            _listener = new TcpListener(ip, port);
            _connectionManager = new ConnectionManager(maxConnection, connectionTimeout);
            _cts = new CancellationTokenSource();
        }

        public async Task StartAsync()
        {
            _listener.Start();
            _isRunning = true;
            Console.WriteLine($"서버가 {_listener.LocalEndpoint}에서 시작됨");

            try
            {
                while (!_cts.IsCancellationRequested && _isRunning)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync();
                    Console.WriteLine($"새 클라이언트 연결: {client.Client.RemoteEndPoint}");

                    // 비동기 클라이언트 처리 시작
                    _ = HandleClientAsync(client);
                }
            }
            catch (ObjectDisposedException)
            {
                // 정상 종료 시 발생할 수 있음.
            }
            catch (Exception ex)
            {
                Console.WriteLine($"서버 오류: {ex.Message}");
            }
            finally
            {
                _listener?.Stop();
                _connectionManager?.Dispose();
                _isRunning = false;
                Console.WriteLine("서버 리스너 종료됨");
            }
        }

        private async Task HandleClientAsync(TcpClient client)
        {
            ClientConnection connection = null;

            try
            {
                connection = await _connectionManager.AcceptConnectionAsync(client);
                Console.WriteLine($"클라이언트 연결 초기화 완료 : {connection._connectionID}");

                // 연결이 종료될 때까지 대기
                await connection.WaitForDisconnectionAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"클라이언트 처리 오류 : {ex.Message}");
                connection?.Dispose();
            }
            finally
            {
                if (connection != null)
                {
                    _connectionManager.RemoveConnection(connection._connectionID);
                    Console.WriteLine($"클라이언트 연결 종료 : {connection._connectionID}");
                }
            }
        }

        public int GetConnectionCount()
        {
            return _connectionManager.GetConnectionCount();
        }

        public void Stop()
        {
            _isRunning = false;
            _cts.Cancel();
            _listener?.Stop();
        }
    }*/

    public class ConnectionManager : IDisposable
    {
        private readonly ConcurrentDictionary<string, ClientConnection> _connectionList = 
            new ConcurrentDictionary<string, ClientConnection>();
        private readonly SocketPool _socketPool;
        private readonly Timer _healthCheckTimer;
        private readonly TimeSpan _connectionTimeout;
        private bool _disposed = false;

        public ConnectionManager(Int32 maxConnections, TimeSpan connectionTimeout)
        {
            _socketPool = new SocketPool(maxConnections);
            _connectionTimeout = connectionTimeout;

            _healthCheckTimer = new Timer(CheckConnections, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
        }

        public async Task<ClientConnection> AcceptConnectionAsync(TcpClient client)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ConnectionManager));

            string connectionID = Guid.NewGuid().ToString("N")[..8];    //짧은 ID 사용
            ClientConnection clientConnection = new ClientConnection(connectionID, client);

            if(_connectionList.TryAdd(connectionID, clientConnection))
            {
                Console.WriteLine($"새 연결 추가 : {connectionID} (총 연결 수 {_connectionList.Count})");

                // 연결 초기화 작업
                await clientConnection.InitializeAsync();
                return clientConnection;
            }
            else
            {
                clientConnection.Dispose();
                throw new InvalidOperationException("연결을 추가할 수 없습니다.");
            }
        }

        public bool RemoveConnection(string connectionID)
        {
            if(_connectionList.TryRemove(connectionID, out ClientConnection clientConnection))
            {
                clientConnection.Dispose();
                Console.WriteLine($"연결 제거됨: {connectionID} (총 연결 수: {_connectionList.Count})");
                return true;
            }

            return false;
        }

        public int GetConnectionCount()
        {
            return _connectionList.Count;
        }

        private void CheckConnections(object state)
        {
            if (_disposed)
                return;

            List<string> disconnectedConnectionList = new List<string>();
            DateTime now = DateTime.UtcNow;

            foreach(var kvp in _connectionList)
            {
                ClientConnection connection = kvp.Value;
                TimeSpan timeSinceLastActivity = now - connection._lastActivity;

                // 마지막 활동 시간이 타임아웃을 초과하면 연결 종료
                if(timeSinceLastActivity > _connectionTimeout)
                {
                    Console.WriteLine($"연결 {connection._connectionID} 시간 초과로 종료 (마지막 활동: {timeSinceLastActivity.TotalMinutes:F1}분 전)");
                    disconnectedConnectionList.Add(connection._connectionID);
                }
                else if(connection._state == ConnectionState.Connected)
                {
                    // 주기적으로 연결 상태 확인 메시지 전송
                    _ = connection.SendHeartbeatAsync();
                }
            }

            foreach(string disconnectedConnection in disconnectedConnectionList)
            {
                RemoveConnection(disconnectedConnection);
            }

            if(_connectionList.Count > 0)
            {
                Console.WriteLine($"헬스체크 완료 - 활성 연결: {_connectionList.Count}개, 제거된 연결: {disconnectedConnectionList.Count}개");
            }
        }
        public void Dispose()
        {
            if (_disposed) return;

            _disposed = true;
            _healthCheckTimer?.Dispose();

            foreach (var connection in _connectionList.Values)
            {
                connection.Dispose();
            }

            _connectionList?.Clear();
            _socketPool?.Clear();
            Console.WriteLine("ConnectionManager가 정리됨");
        }
    }

    public class ClientConnection : IDisposable
    {
        public string _connectionID;
        public DateTime _lastActivity { get; private set; }
        public ConnectionState _state { get; private set; }

        private readonly TcpClient _client;
        private readonly NetworkStream _stream;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly TaskCompletionSource<bool> _disconnectionTcs = new TaskCompletionSource<bool>();
        private bool _disposed = false;

        public ClientConnection(string connectionID, TcpClient client)
        {
            _connectionID = connectionID;
            _client = client;
            _stream = client.GetStream();
            _lastActivity = DateTime.UtcNow;
            _state = ConnectionState.New;
        }

        public async Task InitializeAsync()
        {
            if (_disposed)
                return;

            // 연결 초기화 로직
            _state = ConnectionState.Initializing;
            Console.WriteLine($"클라이언트 {_connectionID} 초기화 시작");

            try
            {
                //클라이언트 환영 메시지 전송
                string welcomMsg = $"연결 성공! 당신의 ID : {_connectionID}";
                byte[] welcomMessage = Encoding.UTF8.GetBytes(welcomMsg);
                await _stream.WriteAsync(welcomMessage, 0, welcomMessage.Length, _cts.Token);

                _state = ConnectionState.Connected;
                _lastActivity = DateTime.UtcNow;
                Console.WriteLine($"클라이언트 {_connectionID} 초기화 완료");

                // 비동기적으로 메시지 수신 시작
                _ = RecevieMessagesAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"클라이언트 {_connectionID} 초기화 실패 : {ex.Message}");
                _state = ConnectionState.Disconnected;
                _disconnectionTcs.TrySetResult(true);
                throw;
            }
        }

        private async Task RecevieMessagesAsync()
        {
            byte[] buffer = new byte[4096];

            try
            {
                while (!_cts.IsCancellationRequested && _state == ConnectionState.Connected)
                {
                    int bytesRead = await _stream.ReadAsync(buffer, 0, buffer.Length, _cts.Token);

                    if (bytesRead == 0)
                    {
                        // 클라이언트 연결 종료
                        Console.WriteLine($"클라이언트 {_connectionID}가 연결을 종료");
                        _state = ConnectionState.Disconnected;
                        break;
                    }

                    // 메시지 처리
                    ProcessoMessage(buffer, bytesRead);

                    // 활동 시간 업데이트
                    _lastActivity = DateTime.UtcNow;
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"클라이언트 {_connectionID} 메시지 수신이 취소됨");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"클라이언트 {_connectionID} 메시지 수신 오류: {ex.Message}");
            }
            finally
            {
                _state = ConnectionState.Disconnected;
                _disconnectionTcs.TrySetResult(true);
            }
        }

        private void ProcessoMessage(byte[] buffer, int length)
        {
            //실제 메시지 처리 로직
            string message = Encoding.UTF8.GetString(buffer, 0, length).Trim();
            Console.WriteLine($"[{_connectionID}] 수신 : {message}");

            if(!string.IsNullOrEmpty(message) && !_disposed)
            {
                var response = $"에코: {message}";
                _ = SendMessageAsync(response);
            }
        }

        private async Task SendMessageAsync(string message)
        {
            if (_disposed || _state != ConnectionState.Connected) return;

            try
            {
                byte[] data = Encoding.UTF8.GetBytes(message);
                await _stream.WriteAsync(data, 0, data.Length, _cts.Token);
                Console.WriteLine($"[{_connectionID}] 전송: {message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"클라이언트 {_connectionID} 메시지 전송 실패: {ex.Message}");
                _state = ConnectionState.Disconnected;
            }
        }

        public async Task SendHeartbeatAsync()
        {
            if (_disposed || _state != ConnectionState.Connected)
                return;

            try
            {
                byte[] heartbeat = Encoding.UTF8.GetBytes("PING");
                await _stream.WriteAsync(heartbeat, 0, heartbeat.Length, _cts.Token);
                Console.WriteLine($"[{_connectionID} 하트비트 전송]");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"클라이언트 {_connectionID} 하트비트 전송 실패 : {ex.Message}");
                _state = ConnectionState.Disconnected;
                _disconnectionTcs.TrySetResult(true);
            }
        }

        public Task WaitForDisconnectionAsync()
        {
            return _disconnectionTcs.Task;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _state = ConnectionState.Disconnected;

            _cts.Cancel();
            _disconnectionTcs.TrySetResult(true);

            try
            {
                _stream?.Dispose();
            }
            catch { }

            try
            {
                _client?.Dispose();
            }
            catch { }

            _cts.Dispose();
            Console.WriteLine($"클라이언트 {_connectionID} 리소스 정리 완료");
        }
    }

    public class SocketPool
    {
        private readonly Int32 _maxConnections;
        public SocketPool(Int32 maxConnections)
        {
            _maxConnections = maxConnections;
        }

        public void Clear()
        {
            // 소켓 풀 정리 로직
            Console.WriteLine("SocketPool 정리됨");
        }
    }

    public enum ConnectionState
    {
        New,
        Initializing,
        Connected,
        Disconnected
    }
}
