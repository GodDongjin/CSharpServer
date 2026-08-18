using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

using MySqlConnector;

namespace ChatServer.Core.DataBase
{
    ////////////////////////////////////////////////////////////////
    ///
    /// Class : DbWorker
    /// Info : DB 작업을 순차 처리하는 비동기 Worker
    ///        JobQueue에 작업할 Job을 등록하고 Worker가 Job을 하나씩 꺼내어 작업을 수행한다.
    ///        EnqueueAsync() :  JobQueue에 작업을 등록
    ///        RunAsync() :  worker가 JobQueue에서 job을 하나씩 꺼내어 작업 진행.
    ///        StopAsync() : 새 작업 등록을 막고 진행 중인 기존 Queue 작업이 끝날 때까지 대기한 후 Worker를 종료한다.
    ///        
    ////////////////////////////////////////////////////////////////
    internal sealed class DbWorker : IAsyncDisposable
    {
        private readonly Int32 _workerID;
        
        // DataBase의 MySqlDataSource를 담을 변수
        private readonly MySqlDataSource _dataSource;

        // Job 순차 등록 Queue
        private readonly ConcurrentQueue<IDbJob> _jobQueue = new();

        // 작업이 들어올 때 worker를 깨울 신호
        private readonly SemaphoreSlim _jobSignal = new(0);

        private readonly object _stateLock = new object();

        // 작업을 처리해줄 worker Task
        private Task? _workerTask;

        // worker 실행
        private bool _isStart;

        // 작업 등록 허용 flag
        private bool _acceptingJobs;
        private bool _disposed;

        public int WorkerID => _workerID;
        public int PendingJobCount => _jobQueue.Count;

        // DBWorker 초기화 함수 : WorkerID 와 MySqlDataSource 등록.
        public DbWorker(int workerId, MySqlDataSource dataSource)
        {
            ArgumentNullException.ThrowIfNull(dataSource);

            _workerID = workerId;
            _dataSource = dataSource;
        }


        // Worker 실행 함수 : worker Task 실행 시킨다.
        public void Start()
        {
            lock(_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                if (_isStart)
                    return;

                _isStart = true;
                _acceptingJobs = true;

                _workerTask = RunAsync();

            }

            Console.WriteLine($"DB Worker {_workerID} 시작");
        }

        // 외부에서 Job 등록 함수
        // Func<MySqlConnection, CancellationToken, Task<T>> operation : 작업할 함수 등록 
        // CancellationToken cancellationToken = default : Task 취소 토큰
        public Task<T> EnqueueAsync<T>(Func<MySqlConnection, CancellationToken, Task<T>> operation, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(operation);

            // job 생성
            var job = new DbJob<T>(operation, cancellationToken);

            // 경쟁 상태 방지 Lock
            lock(_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                if(!_isStart)
                {
                    throw new InvalidOperationException($"DB Worker {_workerID}가 시작되지 않았습니다.");
                }

                if(!_acceptingJobs)
                {
                    throw new InvalidOperationException($"DB Worker {_workerID}가 종료 중입니다.");
                }

                // jobQueue에 job 등록.
                _jobQueue.Enqueue(job);


                // 대기 중인 Worker 깨움
                _jobSignal.Release();
            }

            // job Task 완료 결과 받기위한 Task 반환.
            return job.Completion;
        }


        public async Task RunAsync()
        {
            while(true)
            {
                // 작업이 들어오거나 Stop 신호가 발생할 때까지 대기.
                await _jobSignal.WaitAsync();

                if(_jobQueue.TryDequeue(out IDbJob? job))
                {
                    // await로 작업 순서 보장
                    await job.ExecuteAsync(_dataSource, CancellationToken.None);

                    continue;
                }

                lock(_stateLock)
                {
                    // 더 이상 작업을 받지 않고 큐도 비어있으면 종료
                    if(!_acceptingJobs && _jobQueue.IsEmpty)
                    {
                        break;
                    }
                }
            }

            Console.WriteLine($"DB Worker {_workerID} 종료");
        }

        public async Task StopAsync()
        {
            Task? workerTask;

            // Worker를 정리하기 전에 Lock을 걸어 경쟁 상태를 막는다.
            lock(_stateLock)
            {
                if (!_isStart)
                    return;

                if(_acceptingJobs)
                {
                    _acceptingJobs = false;

                    _jobSignal.Release();
                }

                workerTask = _workerTask;
            }

            // 진행 중인 worker가 있으면 작업이 끝날 때까지 대기.
            if(workerTask is not null)
                await workerTask;

            lock(_stateLock)
            {
                _isStart = false;
                _workerTask = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            await StopAsync();

            _disposed = true;
            _jobSignal.Dispose();
        }
    }

    ////////////////////////////////////////////////////////////////
    ///
    /// Class : DbWorkerPool
    /// Info : DbWorker를 관리하는 Pool이며 Worker 생성 및 생명주기를 관리한다.
    ///        DbWorkerPool() 생성자 : Worker 개수를 지정해 미리 Worker를 생성한다.
    ///        RegisterJob() : 알맞은 Worker ID에 Job을 등록한다.
    ///        RegisterUserJobAsync() : 자동으로 Worker를 선택해 Job을 등록한다.
    ///        
    ////////////////////////////////////////////////////////////////

    public sealed class DbWorkerPool : IAsyncDisposable
    {
        // Worker 관리 배열
        private readonly DbWorker[] _workers;
        private bool _disposed;

        public int WorkerCount => _workers.Length;

        // WorkerPool 생성자 : Worker를 생성하고 WorkerPool에 등록한다.
        // Int32 workerCount : 생성할 worker 개수
        // MySqlDataSource dataSource : worker에 등록할 dataSource
        public DbWorkerPool(Int32 workerCount, MySqlDataSource dataSource)
        {            
            if(workerCount <= 0) {
                throw new ArgumentOutOfRangeException(nameof(workerCount), "Worker 개수는 1개 이상이어야 합니다.");
            }

            ArgumentNullException.ThrowIfNull(dataSource);

            _workers = new DbWorker[workerCount];

            for (int workerID = 0; workerID < workerCount; workerID++)
            {
                DbWorker _dbWorker = new DbWorker(workerID, dataSource);

                _workers[workerID] = _dbWorker;

                _dbWorker.Start();
            }
        }

        public Task<T> RegisterJob<T>(Int32 workerID, Func<MySqlConnection, CancellationToken, Task<T>> operation, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(operation);
            ObjectDisposedException.ThrowIf(_disposed, this);

            if((UInt32)workerID >= (UInt32)_workers.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(workerID), workerID, "존재하지 않는 DB Worker ID입니다.");
            }

            return _workers[workerID].EnqueueAsync(operation, cancellationToken);
        }

        // Job 등록 외부 함수이며 동일 userID는 항상 같은 워커로 매핑되어 그 유저의 작업 처리 순서를 보장한다.
        // UInt64 userID : 동일 사용자의 작업을 같은 Worker로 보내기 위한 라우팅 키
        // Func<MySqlConnection, CancellationToken, Task<T>> operation : worker에 등록할 job 함수.
        // CancellationToken cancellationToken = default : 취소 토큰
        public Task<T> RegisterUserJobAsync<T>(UInt64 userID, Func<MySqlConnection, CancellationToken, Task<T>> operation, CancellationToken cancellationToken = default)
        {
            // userID를 이용해 workerID를 구한다.
            Int32 workerID = GetWorkerID(userID);

            // worker에 작업 함수 및 취소 토큰 등록
            return RegisterJob(workerID, operation, cancellationToken);
        }

        public Int32 GetWorkerID(UInt64 userID)
        {
            return (Int32)((UInt64)userID % (UInt64)_workers.Length);
        }

        public async Task StopAsync()
        {
            Task[] stopTasks = _workers.Select(worker => worker.StopAsync()).ToArray();

            await Task.WhenAll(stopTasks);
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;

            await Task.WhenAll(_workers.Select(worker => worker.StopAsync()));

            foreach(DbWorker worker in _workers)
            {
                await worker.DisposeAsync();
            }
        }

    }
}
