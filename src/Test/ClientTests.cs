using System.Net.WebSockets;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;
using TheColdWorld.Utils.WebSocket;

namespace TheColdWorld.Utils.Test;

public class ClientTests : IAsyncLifetime
{
    private WebSocketTestHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = new WebSocketTestHost();
        await _host.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.DisposeAsync();
    }

    private static async Task<bool> WaitForLogAsync(
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs,
        Func<(Logging.LogLevel Level, string Message, Exception? Exception), bool> predicate,
        TimeSpan timeout)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        while (stopwatch.Elapsed < timeout)
        {
            lock (logs)
            {
                if (logs.Any(predicate)) return true;
            }
            await Task.Delay(25);
        }

        lock (logs)
        {
            return logs.Any(predicate);
        }
    }

    private static List<(Logging.LogLevel Level, string Message, Exception? Exception)> SubscribeLogs(
        out Action<Logging.LogLevel, string, Exception?> handler)
    {
        var logs = new List<(Logging.LogLevel Level, string Message, Exception? Exception)>();
        void Handler(Logging.LogLevel level, string message, Exception? exception)
        {
            lock (logs)
            {
                logs.Add((level, message, exception));
            }
        }

        handler = Handler;
        WebSocketClient.OnLogging += handler;
        return logs;
    }

    [Fact]
    public async Task WebSocketClient_NormalClose_IsSilent()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> handler);

        try
        {
            var client = new WebSocketClient(_host.ServerUri);
            await client.ConnectAsync();
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            await client.DisposeAsync();

            Exception? error = await _host.HandleClientCompletion.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Null(error);

            lock (logs)
            {
                Assert.DoesNotContain(
                    logs,
                    l => l.Level is Logging.LogLevel.Warning or Logging.LogLevel.Error);
            }
        }
        finally
        {
            WebSocketClient.OnLogging -= handler;
        }
    }

    [Fact]
    public async Task WebSocketClient_AbnormalClose_LogsWarningWithExceptionMessage()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> handler);

        try
        {
            var client = new WebSocketClient(_host.ServerUri);
            await client.ConnectAsync();
            System.Net.WebSockets.WebSocket serverSocket = await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            serverSocket.Abort();

            ReceiveOutcome outcome = await client
                .ReceiveAsync()
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(10));

            ReceiveOutcome.AbortedOutcome? aborted = outcome.AsAbortedOutcome();
            Assert.NotNull(aborted);
            Assert.NotNull(aborted!.Exception);

            Assert.True(await WaitForLogAsync(
                logs,
                l => l.Level == Logging.LogLevel.Warning,
                TimeSpan.FromSeconds(5)));

            lock (logs)
            {
                var warning = logs.First(l => l.Level == Logging.LogLevel.Warning);
                Assert.Same(aborted.Exception, warning.Exception);
                Assert.Equal(aborted.Exception.Message, warning.Message);
            }

            await client.DisposeAsync();
        }
        finally
        {
            WebSocketClient.OnLogging -= handler;
        }
    }

    [Fact]
    public async Task WebSocketClient_InvalidJson_LogsWarning_AndReturnsInvalidOutcome()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> handler);

        try
        {
            var client = new WebSocketClient(_host.InvalidUri);
            await client.ConnectAsync();

            ReceiveOutcome outcome = await client
                .ReceiveAsync()
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(10));

            ReceiveOutcome.InvalidOutcome? invalid = outcome.AsInvalidOutcome();
            Assert.NotNull(invalid);
            Assert.IsType<InvalidDataException>(invalid!.Exception);
            Assert.Contains("not valid JSON", invalid.Message);

            Assert.True(await WaitForLogAsync(
                logs,
                l => l.Level == Logging.LogLevel.Warning,
                TimeSpan.FromSeconds(5)));

            lock (logs)
            {
                var warning = logs.First(l => l.Level == Logging.LogLevel.Warning);
                Assert.Same(invalid.Exception, warning.Exception);
                Assert.Equal(invalid.Exception.Message, warning.Message);
            }

            await client.DisposeAsync();
        }
        finally
        {
            WebSocketClient.OnLogging -= handler;
        }
    }

    [Fact]
    public async Task WebSocketClient_ValidPacket_ReturnsSuccessOutcome()
    {
        var client = new WebSocketClient(_host.ServerUri);
        await client.ConnectAsync();
        System.Net.WebSockets.WebSocket serverSocket = await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

        try
        {
            var packet = new TestPacket(
                PacketBindSide.ClientBind,
                new("test:s2c"),
                new() { ["message"] = "hello" });

            await serverSocket.SendAsync(
                WebsocketHelper.BuildPacket(packet),
                WebSocketMessageType.Binary,
                true,
                CancellationToken.None);

            ReceiveOutcome outcome = await client
                .ReceiveAsync()
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(10));

            Assert.True(outcome.IsSuccess);

            (Identifier id, JsonObject data) = outcome.AsSuccessOutcome()!.ParsePacket();
            Assert.Equal("test:s2c", id.ToString());
            TestPacket parsed = new(data);
            Assert.Equal("hello", (string?)parsed.Data["message"]?.AsValue());
        }
        finally
        {
            await client.DisposeAsync();
        }
    }
}
