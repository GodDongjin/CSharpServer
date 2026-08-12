using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

using MySqlConnector;

namespace ChatServer.Core.DataBase
{
    internal sealed class DbWorker : IAsyncDisposable
    {
        private readonly Int32 _workerID;
        private readonly MySqlDataSource _dataSource;

        private readonly ConcurrentQueue<IDbJob> _jobQueue = new();

        // 작업이 들어올 때 worker를 깨울 신호
        private readonly SemaphoreSlim _jobSignal = new(0);

        private readonly object _stateLock = new object();

        private Task? _workerTask;

        private bool _isStart;
        private bool _acceptingJobs;
        private bool _disposed;

        public int WorkerID => _workerID;
        public int PendingJobCount => _jobQueue.Count;

        public DbWorker(int workerId, MySqlDataSource dataSource)
        {
            ArgumentNullException.ThrowIfNull(dataSource);

            _workerID = workerId;
            _dataSource = dataSource;
        }


        public void start()
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

        public Task<T> EnqueueAsync<T>(Func<MySqlConnection, CancellationToken, Task<T>> operation, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(operation);

            var job = new DbJob<T>(operation, cancellationToken);


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

                _jobQueue.Enqueue(job);


                // 대기중인 worker 깨움.
                _jobSignal.Release();
            }

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
                    await job.ExcuteAsync(_dataSource, CancellationToken.None);

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

    public sealed class DbWorkerPool
    {
        // Worker 관리 리스트
        private readonly ConcurrentDictionary<Int32, DbWorker> _workerDic = new ConcurrentDictionary<int, DbWorker>();
        
        // Worker에 분배하기전 담을 Queue
        private readonly ConcurrentQueue<IDbJob> _jobQueue = new ConcurrentQueue<IDbJob>();

        private readonly Int32 _maxWorkerCount;
        private readonly SemaphoreSlim _workerSignal;

        public DbWorkerPool(Int32 wroekrCount, MySqlDataSource dataSource)
        {
            _workerSignal = new SemaphoreSlim(wroekrCount, wroekrCount);

            for(int i = 0; i < wroekrCount; i++)
            {
                DbWorker _dbWorker = new DbWorker(i, dataSource);
                if (!_workerDic.TryAdd(_dbWorker.WorkerID, _dbWorker))
                {
                    Console.WriteLine($"WorkerDic에 Worker 생성 실패");
                }

                _dbWorker.start();
            }
        }

        public async Task<T> RegisterJob<T>(Func<MySqlConnection, CancellationToken, Task<T>> operation, Int32 workerID, CancellationToken token)
        {
            DbWorker? dbWorker;

            if (!_workerDic.TryGetValue(workerID, out dbWorker))
            {
                ArgumentNullException.ThrowIfNullOrEmpty($"workerDic에서 TryGetValue 실패 : {workerID}");

                // 여기서 return 시킬게 뭐가 있지?
            }

            return await dbWorker.EnqueueAsync(operation, token);
        }


    }
}
