using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;
using TheColdWorld.Utils.WebSocket;

namespace TheColdWorld.Utils.Test;

public class ServerControlUnitTests : IAsyncLifetime
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
        WebSocketServerControlUnit.OnLogging += handler;
        return logs;
    }

    [Fact]
    public async Task HandleClient_ForceClose_DoesNotThrow_AndLogsWarning()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> handler);

        try
        {
            using var client = new ClientWebSocket();
            await client.ConnectAsync(_host.ServerUri, CancellationToken.None);
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            client.Abort();

            Exception? error = await _host.HandleClientCompletion.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Null(error);

            Assert.True(await WaitForLogAsync(
                logs,
                l => l.Level == Logging.LogLevel.Warning,
                TimeSpan.FromSeconds(5)));

            lock (logs)
            {
                Assert.DoesNotContain(logs, l => l.Level == Logging.LogLevel.Error);

                var warning = logs.First(l => l.Level == Logging.LogLevel.Warning);
                Assert.NotNull(warning.Exception);
                Assert.Equal(warning.Exception!.Message, warning.Message);
            }
        }
        finally
        {
            WebSocketServerControlUnit.OnLogging -= handler;
        }
    }

    [Fact]
    public async Task HandleClient_NormalClose_IsSilent_AndCompletes()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> handler);

        try
        {
            using var client = new ClientWebSocket();
            await client.ConnectAsync(_host.ServerUri, CancellationToken.None);
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            await client
                .CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

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
            WebSocketServerControlUnit.OnLogging -= handler;
        }
    }

    [Fact]
    public async Task HandleClient_InvalidJson_LogsWarning_AndContinues()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> logHandler);

        var received = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task PacketHandler(
            Identifier id,
            JsonObject data,
            WebSocketServerControlUnit.SendToClientAsync send,
            Action stop,
            CancellationToken token)
        {
            received.TrySetResult(data);
            return Task.CompletedTask;
        }

        _host.ControlUnit.OnPacketAccepted += PacketHandler;

        try
        {
            using var client = new ClientWebSocket();
            await client.ConnectAsync(_host.ServerUri, CancellationToken.None);
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            await client.SendAsync(
                Encoding.UTF8.GetBytes("{ this is not json"),
                WebSocketMessageType.Binary,
                true,
                CancellationToken.None);

            Assert.True(await WaitForLogAsync(
                logs,
                l => l.Level == Logging.LogLevel.Warning,
                TimeSpan.FromSeconds(10)));

            lock (logs)
            {
                var warning = logs.First(l => l.Level == Logging.LogLevel.Warning);
                Assert.IsType<InvalidDataException>(warning.Exception);
                Assert.Contains("not valid JSON", warning.Message);
            }

            var packet = new TestPacket(
                PacketBindSide.ServerBind,
                new("test:c2s"),
                new() { ["message"] = "hello" });

            await client.SendAsync(
                WebsocketHelper.BuildPacket(packet),
                WebSocketMessageType.Binary,
                true,
                CancellationToken.None);

            JsonObject data = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
            TestPacket parsed = new(data);
            Assert.Equal("hello", (string?)parsed.Data["message"]?.AsValue());

            await client
                .CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            Exception? error = await _host.HandleClientCompletion.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Null(error);
        }
        finally
        {
            _host.ControlUnit.OnPacketAccepted -= PacketHandler;
            WebSocketServerControlUnit.OnLogging -= logHandler;
        }
    }

    [Fact]
    public async Task HandleClient_TextMessage_LogsWarning_AndContinues()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> logHandler);

        var received = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task PacketHandler(
            Identifier id,
            JsonObject data,
            WebSocketServerControlUnit.SendToClientAsync send,
            Action stop,
            CancellationToken token)
        {
            received.TrySetResult(data);
            return Task.CompletedTask;
        }

        _host.ControlUnit.OnPacketAccepted += PacketHandler;

        try
        {
            using var client = new ClientWebSocket();
            await client.ConnectAsync(_host.ServerUri, CancellationToken.None);
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            await client.SendAsync(
                Encoding.UTF8.GetBytes("hello"),
                WebSocketMessageType.Text,
                true,
                CancellationToken.None);

            Assert.True(await WaitForLogAsync(
                logs,
                l => l.Level == Logging.LogLevel.Warning,
                TimeSpan.FromSeconds(10)));

            lock (logs)
            {
                var warning = logs.First(l => l.Level == Logging.LogLevel.Warning);
                Assert.Contains("non-binary", warning.Message);
            }

            var packet = new TestPacket(
                PacketBindSide.ServerBind,
                new("test:c2s"),
                new() { ["message"] = "after-text" });

            await client.SendAsync(
                WebsocketHelper.BuildPacket(packet),
                WebSocketMessageType.Binary,
                true,
                CancellationToken.None);

            JsonObject data = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
            TestPacket parsed = new(data);
            Assert.Equal("after-text", (string?)parsed.Data["message"]?.AsValue());
        }
        finally
        {
            _host.ControlUnit.OnPacketAccepted -= PacketHandler;
            WebSocketServerControlUnit.OnLogging -= logHandler;
        }
    }

    [Fact]
    public async Task HandleClient_EmptyBinaryMessage_LogsWarning_AndContinues()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> logHandler);

        var received = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task PacketHandler(
            Identifier id,
            JsonObject data,
            WebSocketServerControlUnit.SendToClientAsync send,
            Action stop,
            CancellationToken token)
        {
            received.TrySetResult(data);
            return Task.CompletedTask;
        }

        _host.ControlUnit.OnPacketAccepted += PacketHandler;

        try
        {
            using var client = new ClientWebSocket();
            await client.ConnectAsync(_host.ServerUri, CancellationToken.None);
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            await client.SendAsync(
                Array.Empty<byte>(),
                WebSocketMessageType.Binary,
                true,
                CancellationToken.None);

            Assert.True(await WaitForLogAsync(
                logs,
                l => l.Level == Logging.LogLevel.Warning,
                TimeSpan.FromSeconds(10)));

            lock (logs)
            {
                var warning = logs.First(l => l.Level == Logging.LogLevel.Warning);
                Assert.Contains("empty binary", warning.Message);
            }

            var packet = new TestPacket(
                PacketBindSide.ServerBind,
                new("test:c2s"),
                new() { ["message"] = "after-empty" });

            await client.SendAsync(
                WebsocketHelper.BuildPacket(packet),
                WebSocketMessageType.Binary,
                true,
                CancellationToken.None);

            JsonObject data = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
            TestPacket parsed = new(data);
            Assert.Equal("after-empty", (string?)parsed.Data["message"]?.AsValue());
        }
        finally
        {
            _host.ControlUnit.OnPacketAccepted -= PacketHandler;
            WebSocketServerControlUnit.OnLogging -= logHandler;
        }
    }

    [Fact]
    public async Task HandleClient_BusinessException_IsRethrown_AndLoggedAsError()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> logHandler);

        Task PacketHandler(
            Identifier id,
            JsonObject data,
            WebSocketServerControlUnit.SendToClientAsync send,
            Action stop,
            CancellationToken token)
        {
            throw new InvalidOperationException("boom");
        }

        _host.ControlUnit.OnPacketAccepted += PacketHandler;

        try
        {
            using var client = new ClientWebSocket();
            await client.ConnectAsync(_host.ServerUri, CancellationToken.None);
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            var packet = new TestPacket(
                PacketBindSide.ServerBind,
                new("test:c2s"),
                new() { ["message"] = "hello" });

            await client.SendAsync(
                WebsocketHelper.BuildPacket(packet),
                WebSocketMessageType.Binary,
                true,
                CancellationToken.None);

            Exception? error = await _host.HandleClientCompletion.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var typed = Assert.IsType<InvalidOperationException>(error);
            Assert.Equal("boom", typed.Message);

            Assert.True(await WaitForLogAsync(
                logs,
                l => l.Level == Logging.LogLevel.Error,
                TimeSpan.FromSeconds(5)));

            lock (logs)
            {
                var errorLog = logs.First(l => l.Level == Logging.LogLevel.Error);
                Assert.Same(typed, errorLog.Exception);
            }
        }
        finally
        {
            _host.ControlUnit.OnPacketAccepted -= PacketHandler;
            WebSocketServerControlUnit.OnLogging -= logHandler;
        }
    }

    [Fact]
    public async Task HandleClient_Cancellation_IsSilent()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> handler);

        try
        {
            using var client = new ClientWebSocket();
            await client.ConnectAsync(_host.ServerUri, CancellationToken.None);
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            _host.CancelServer();

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
            WebSocketServerControlUnit.OnLogging -= handler;
        }
    }

    [Fact]
    public async Task HandleClient_HandlerSendAfterClientAbort_DoesNotThrow()
    {
        List<(Logging.LogLevel Level, string Message, Exception? Exception)> logs =
            SubscribeLogs(out Action<Logging.LogLevel, string, Exception?> logHandler);

        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var continueSend = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task PacketHandler(
            Identifier id,
            JsonObject data,
            WebSocketServerControlUnit.SendToClientAsync send,
            Action stop,
            CancellationToken token)
        {
            handlerEntered.TrySetResult();
            await continueSend.Task;

            var reply = new TestPacket(
                PacketBindSide.ClientBind,
                new("test:s2c"),
                new() { ["message"] = "reply" });

            await send(reply, token);
        }

        _host.ControlUnit.OnPacketAccepted += PacketHandler;

        try
        {
            using var client = new ClientWebSocket();
            await client.ConnectAsync(_host.ServerUri, CancellationToken.None);
            await _host.ServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));

            var packet = new TestPacket(
                PacketBindSide.ServerBind,
                new("test:c2s"),
                new() { ["message"] = "trigger" });

            await client.SendAsync(
                WebsocketHelper.BuildPacket(packet),
                WebSocketMessageType.Binary,
                true,
                CancellationToken.None);

            await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));

            client.Abort();
            continueSend.TrySetResult();

            Exception? error = await _host.HandleClientCompletion.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Null(error);

            Assert.True(await WaitForLogAsync(
                logs,
                l => l.Level == Logging.LogLevel.Warning,
                TimeSpan.FromSeconds(5)));
        }
        finally
        {
            _host.ControlUnit.OnPacketAccepted -= PacketHandler;
            WebSocketServerControlUnit.OnLogging -= logHandler;
        }
    }
}