using System;
using System.Collections.Concurrent;
using MySqlConnector;

namespace ChatServer.Core.DataBase
{
    public sealed class MysqlDataBase : IAsyncDisposable
    {
        // MySqlConnector 
        private readonly MySqlDataSource _dataSource;
        private readonly ConcurrentQueue<Action> _jobQueue = new();

        private Thread? _workerThread;

        private volatile bool _isRunning;
        private volatile bool _isConnect;
        private bool _disposed;

        public MysqlDataBase(string connectionString)
        {
            ArgumentNullException.ThrowIfNullOrWhiteSpace(connectionString);

            //MySqlDataSourceBuilder를 통해 connectionString으로 설정된 구성으로 생성된 새로운 MySqlDataSource를 반환해준다.
            _dataSource = new MySqlDataSourceBuilder(connectionString)
                .Build();
        }
        
        public async Task<bool> IsConnect(CancellationToken cancelToken = default)
        {
            try
            {
                await using MySqlConnection connection = 
                    await _dataSource.OpenConnectionAsync(cancelToken);

                await using MySqlCommand command =
                    connection.CreateCommand();

                command.CommandText = "SELECT 1;";

                await command.ExecuteNonQueryAsync(cancelToken);

                _isConnect = true;

                Console.WriteLine("DB 연결 성공");

                return true;
            }
            catch(MySqlException exception)
            {
                _isConnect = false;

                Console.WriteLine($"MySql 연결 실패 : {exception.Message}");
                return false;
            }
        }

        public void Start()
        {
            if (_isRunning)
                return;

            if (!_isConnect)
            {
                throw new InvalidOperationException("DB 연결 검사가 완료되지 않았습니다.");
            }

            _isRunning = true;

            _workerThread = new Thread(Worker)
            {
                Name = "MySQL Worker",
                IsBackground = false
            };

            _workerThread.Start();

            Console.WriteLine("DB worker 시작");
        }


        public void PushJop(Action job)
        {
            ArgumentNullException.ThrowIfNull(job);

            if (!_isRunning)
            {
                throw new InvalidOperationException("DB Worker가 실행 중이지 않습니다.");
            }

            _jobQueue.Enqueue(job);
        }

        public void Worker()
        {
           while(_isRunning)
            {
                if(_jobQueue.TryDequeue(out Action job))
                {
                    try
                    {
                        job();
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine($"DB Job 처리 실패 : {ex}");
                    }

                    continue;
                }

                // 등록된 Job이 없으면 잠시 대기.
                Thread.Sleep(10);
            }

            Console.WriteLine("DB Worker 종료");
        }


        public void Stop()
        {
            if (!_isRunning)
                return;

            _isRunning = false;

            if(_workerThread is not null &&
                _workerThread.IsAlive &&
                Thread.CurrentThread != _workerThread)
            {
                _workerThread.Join();
            }

            _workerThread = null;
        }

        public async ValueTask DisposeAsync() 
        {
            if (_disposed)
                return;

            _disposed = true;

            Stop();

            _isConnect = false;

            // 연결 해제.
            await _dataSource.DisposeAsync();
        }
    }
}
