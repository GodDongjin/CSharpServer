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

    public sealed class DbWorkerPool : IAsyncDisposable
    {
        // Worker 관리 리스트
        private readonly DbWorker[] _workers;
        private bool _disposed;


        public int WorkerCount => _workers.Length;
        public DbWorkerPool(Int32 workerCount, MySqlDataSource dataSource)
        {
            
            if(workerCount <= 0) {
                throw new ArgumentOutOfRangeException(nameof(workerCount), "Worker 개수는 1개 이상이어야 합나디.");
            }

            ArgumentNullException.ThrowIfNull(dataSource);

            _workers = new DbWorker[workerCount];

            for (int workerID = 0; workerID < workerCount; workerID++)
            {
                DbWorker _dbWorker = new DbWorker(workerID, dataSource);

                _workers[workerID] = _dbWorker;

                _dbWorker.start();
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

        public Task<T> RegisterUserJobAsync<T>(UInt64 userID, Func<MySqlConnection, CancellationToken, Task<T>> operation, CancellationToken cancellationToken = default)
        {
            Int32 workerID = GetWorkerID(userID);

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
