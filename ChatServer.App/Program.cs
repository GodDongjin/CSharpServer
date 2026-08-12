using System.Diagnostics;
using System.Net;
using ChatServer.App.ChatSession;
using ChatServer.App.DataBase;
using ChatServer.App.Packet;
using ChatServer.Core.DataBase;
using ChatServer.Core.Network;

namespace ChatServer.App;

internal static class Program
{
    private const string ConnectionStringVariable =
        "GAME_DB_CONNECTION_STRING";

    private const int ConcurrentTestRequestCount = 100_000;

    private static async Task Main(string[] args)
    {
        string? connectionString =
            Environment.GetEnvironmentVariable(
                ConnectionStringVariable);

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.WriteLine(
                $"Set {ConnectionStringVariable} before starting ChatServer.");
            return;
        }

        await using var database = new MysqlDataBase(
            connectionString,
            workerCount: 4);

        if (!await database.CheckConnectAsync())
        {
            Console.WriteLine("Database connection check failed.");
            return;
        }

        var repository = new Repository(database);

        var sessionManager = new SessionManager(
            maxSessionCount: 1000,
            () => new GameSession());

        var packetHandler = new GSPacketHandler();
        packetHandler.Initialize();

        var chatService = new Service(
            SERVICE_TYPE.CHAT_SERVER,
            sessionManager,
            packetHandler,
            maxConnection: 1000);

        IPAddress ipAddress = IPAddress.Parse("127.0.0.1");
        chatService.StartServer(ipAddress, 7777);

        Console.WriteLine(
            "ChatServer started. Commands: dbtest, dbload, status, quit");

        while (true)
        {
            string? input = Console.ReadLine()?.Trim();

            if (string.Equals(
                    input,
                    "quit",
                    StringComparison.OrdinalIgnoreCase))
            {
                break;
            }

            if (string.Equals(
                    input,
                    "dbtest",
                    StringComparison.OrdinalIgnoreCase))
            {
                await RunDatabaseTestAsync(repository);
                continue;
            }

            if (string.Equals(
                    input,
                    "dbload",
                    StringComparison.OrdinalIgnoreCase))
            {
                await RunConcurrentDatabaseTestAsync(repository);
                continue;
            }


            if (string.Equals(
                    input,
                    "status",
                    StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("ChatServer is running.");
            }
        }

        Console.WriteLine("Stopping ChatServer...");
        chatService.StopServer();
        Console.WriteLine("ChatServer stopped.");
    }

    private static async Task RunDatabaseTestAsync(
        Repository repository)
    {
        using var timeoutSource =
            new CancellationTokenSource(
                TimeSpan.FromSeconds(5));

        var stopwatch = Stopwatch.StartNew();

        try
        {
            DBTest result = await repository.DBTestAsync(
                userID: 1,
                timeoutSource.Token);

            stopwatch.Stop();

            Console.WriteLine(
                $"DB test success: TestValue={result.TestValue}, " +
                $"elapsed={stopwatch.Elapsed.TotalMilliseconds:F2} ms");
        }
        catch (OperationCanceledException)
            when (timeoutSource.IsCancellationRequested)
        {
            Console.WriteLine("DB test timed out after 5 seconds.");
        }
        catch (Exception exception)
        {
            Console.WriteLine(
                $"DB test failed: {exception.Message}");
        }
    }

    private static async Task RunConcurrentDatabaseTestAsync(
        Repository repository)
    {
        using var timeoutSource =
            new CancellationTokenSource(
                TimeSpan.FromMinutes(2));

        Console.WriteLine(
            $"Registering {ConcurrentTestRequestCount:N0} DB jobs...");

        var stopwatch = Stopwatch.StartNew();

        try
        {
            Task<DBTest>[] tasks = Enumerable
                .Range(0, ConcurrentTestRequestCount)
                .Select(index => repository.DBTestAsync(
                    userID: (ulong)(index + 1),
                    timeoutSource.Token))
                .ToArray();

            DBTest[] results = await Task.WhenAll(tasks);

            stopwatch.Stop();

            int invalidResultCount = results.Count(
                result => result.TestValue != 1);

            double requestsPerSecond =
                results.Length / stopwatch.Elapsed.TotalSeconds;

            double processingCostMilliseconds =
                stopwatch.Elapsed.TotalMilliseconds / results.Length;

            Console.WriteLine(
                $"DB load test completed.{Environment.NewLine}" +
                $"Requests: {results.Length:N0}{Environment.NewLine}" +
                $"Elapsed: {stopwatch.Elapsed.TotalSeconds:F2} s{Environment.NewLine}" +
                $"Throughput: {requestsPerSecond:N0} req/s{Environment.NewLine}" +
                $"Processing cost: {processingCostMilliseconds:F4} ms/request{Environment.NewLine}" +
                $"Invalid results: {invalidResultCount:N0}");
        }
        catch (OperationCanceledException)
            when (timeoutSource.IsCancellationRequested)
        {
            stopwatch.Stop();

            Console.WriteLine(
                $"DB load test timed out after " +
                $"{stopwatch.Elapsed.TotalSeconds:F2} seconds.");
        }
        catch (Exception exception)
        {
            stopwatch.Stop();

            Console.WriteLine(
                $"DB load test failed after " +
                $"{stopwatch.Elapsed.TotalSeconds:F2} seconds: " +
                exception);
        }
    }
}
