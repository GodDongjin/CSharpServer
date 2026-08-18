using System;
using System.Collections.Concurrent;
using MySqlConnector;

//////////////////////////////////////////////////
///
///     Class : DataBase
///     설명 : 데이터베이스의 기본 구성이며 _dataSource를 이용해 DB Connection을 풀링 방식으로 관리한다.
///     Worker와 JobQueue를 이용해 Worker에게 작업을 맡기는 방식의 클래스이다.
///     Connection String의 MinimumPoolSize로 초기 Connection 최소 유지 수를 정한다.
///     workerCount로 Worker 수를 지정한다.
///
/////////////////////////////////////////////////


namespace ChatServer.Core.DataBase
{
    public sealed class MysqlDataBase : IAsyncDisposable
    {
        // MySqlConnector를 pool방식으로 관리 및
        // dataSource.OpenConnectionAsync으로 pool에 있는 connection을 빌려 올 수 있다.
        private readonly MySqlDataSource _dataSource;
        private readonly DbWorkerPool _workerPool;

        private bool _disposed;

        public MysqlDataBase(string connectionString, Int32 workerCount)
        {
            ArgumentNullException.ThrowIfNullOrWhiteSpace(connectionString);

            if(workerCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(workerCount));
            }

            //MySqlDataSourceBuilder를 통해 connectionString으로 설정된 구성으로 생성된 새로운 MySqlDataSource를 반환해준다.
            _dataSource = new MySqlDataSourceBuilder(connectionString)
                .Build();

            _workerPool = new DbWorkerPool(workerCount, _dataSource);
        }
        
        public async Task<bool> CheckConnectAsync(CancellationToken cancelToken = default)
        {
            try
            {
                // MySqlDataSource에서 connection을 빌려와 DB작업 진행.
                await using MySqlConnection connection = await _dataSource.OpenConnectionAsync(cancelToken);

                await using MySqlCommand command = connection.CreateCommand();

                command.CommandText = "SELECT 1;";

                object? result = await command.ExecuteScalarAsync(cancelToken);

                return Convert.ToInt32(result) == 1;
            }
            catch(MySqlException exception)
            {
                Console.WriteLine($"MySql 연결 실패 : {exception.Message}");

                return false;
            }
        }


        // 외부에서 DB 작업을 요청하기 위한 외부 함수.
        public Task<T> ExecuteAsync<T>(UInt64 userID/*호출한 유저 ID*/, Func<MySqlConnection, CancellationToken, Task<T>> operation /*요청할 작업 함수*/, CancellationToken cancellationToken = default/*취소 객체*/)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            // 요청할 작업을 WorkQueue에 등록 함수
            return _workerPool.RegisterUserJobAsync(userID, operation, cancellationToken);
        }
      
        public async ValueTask DisposeAsync() 
        {
            if (_disposed)
                return;

            _disposed = true;

            // 연결 해제.
            await _workerPool.DisposeAsync();
            await _dataSource.DisposeAsync();
        }
    }
}
