using System;
using System.Collections.Generic;
using System.Text;
using MySqlConnector;

namespace ChatServer.Core.DataBase
{
    ////////////////////////////////////////////////////////////////////////

    ///worker가 반환 자료형과 관계없이 DB 작업을 처리하기 위한 인터페이스이다.

    ////////////////////////////////////////////////////////////////////////
    public interface IDbJob
    {
        Task ExcuteAsync(
            MySqlDataSource dataSource,
            CancellationToken workerCancelToken
            );
    }

    internal sealed class DbJob<T> : IDbJob
    {
        private readonly Func<MySqlConnection, CancellationToken, Task<T>> _operation;

        private readonly CancellationToken _cancelToken;

        private readonly TaskCompletionSource<T> _copletionSource =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        // 호출자가 기다릴 DB 작업의 결과 반환
        public Task<T> Completion => _copletionSource.Task;

        public DbJob(
            Func<MySqlConnection, CancellationToken, Task<T>> operation,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(operation);

            _operation = operation;
            _cancelToken = cancellationToken;
        }

        public async Task ExcuteAsync(MySqlDataSource dataSource,
            CancellationToken workerCancelToken)
        {
            ArgumentNullException.ThrowIfNull(dataSource);

            using CancellationTokenSource linkedCancellationSource =
                CancellationTokenSource.CreateLinkedTokenSource(_cancelToken, workerCancelToken);

            CancellationToken cancellationToken = linkedCancellationSource.Token;

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                await using MySqlConnection connection =
                    await dataSource.OpenConnectionAsync(cancellationToken);

                T result = await _operation(connection, cancellationToken);

                _copletionSource.TrySetResult(result);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _copletionSource.TrySetCanceled(cancellationToken);
            }
            catch (Exception ex)
            {
                _copletionSource.TrySetException(ex);
            }
        }

    }
}
