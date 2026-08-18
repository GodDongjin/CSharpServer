using System;
using System.Collections.Generic;
using System.Text;
using MySqlConnector;

namespace ChatServer.Core.DataBase
{
    ////////////////////////////////////////////////////////////////////////

    ///worker가 반환 자료형과 관계없이 DB 작업을 처리하기 위한 인터페이스이다.

    ////////////////////////////////////////////////////////////////////////
    internal interface IDbJob
    {
        Task ExecuteAsync(
            MySqlDataSource dataSource,
            CancellationToken workerCancelToken
            );
    }

    ////////////////////////////////////////////////////////////////
    ///
    /// Class : DbJob<T>
    /// Info : 실제 worker가 작업할 내용을 담을 객체
    ///        ExecuteAsync() : Worker가 실제 작업을 수행하는 함수.
    ///        
    ////////////////////////////////////////////////////////////////
    ///
    internal sealed class DbJob<T> : IDbJob
    {
        // 작업 내용
        private readonly Func<MySqlConnection, CancellationToken, Task<T>> _operation;

        private readonly CancellationToken _cancelToken;

        // DB 작업의 결과, 예외 또는 취소 상태를
        // 호출자의 Completion Task에 전달한다.
        private readonly TaskCompletionSource<T> _completionSource =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        // 호출자가 기다릴 DB 작업의 결과 반환
        public Task<T> Completion => _completionSource.Task;

        public DbJob(
            Func<MySqlConnection, CancellationToken, Task<T>> operation,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(operation);

            _operation = operation;
            _cancelToken = cancellationToken;
        }

        // 등록된 작업 실행 
        public async Task ExecuteAsync(MySqlDataSource dataSource,
            CancellationToken workerCancelToken)
        {
            ArgumentNullException.ThrowIfNull(dataSource);

            //job의 취소토큰이랑 worker의 취소 토큰은 합치는 작업.
            using CancellationTokenSource linkedCancellationSource =
                CancellationTokenSource.CreateLinkedTokenSource(_cancelToken, workerCancelToken);

            CancellationToken cancellationToken = linkedCancellationSource.Token;

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                // MySqlDataSource의 pool에서 connection을 빌려온다.
                await using MySqlConnection connection =
                    await dataSource.OpenConnectionAsync(cancellationToken);

                // connection을 진행할 작업에 넘겨줘 DB작업 진행.
                T result = await _operation(connection, cancellationToken);

                // Completion Task를 성공 상태로 완료하고 결과를 전달한다.
                _completionSource.TrySetResult(result);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Task 실행 취소
                _completionSource.TrySetCanceled(cancellationToken);
            }
            catch (Exception ex)
            {
                // Task 실행 중 실패
                _completionSource.TrySetException(ex);
            }
        }

    }
}
