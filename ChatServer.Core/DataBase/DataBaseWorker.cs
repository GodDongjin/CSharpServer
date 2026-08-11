using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

using MySqlConnector;

namespace ChatServer.Core.DataBase
{
    internal sealed class DbWorker : IAsyncDisposable
    {
        private readonly int _workerID;
        private readonly MySqlDataSource _dataSource;

        private readonly ConcurrentQueue<IDbJob> _jobQueue = new();

        // 작업이 들어올 때 worker를 깨울 신호
        private readonly SemaphoreSlim _jobSingl = new(0);

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

                //_workerTask = RunAsync();

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

                _jobSingl.Release();
            }

            return job.Completion;
        }


        public async Task DisoperAsync()
        {

        }
    }
}
