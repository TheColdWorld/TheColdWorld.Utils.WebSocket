using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;
using TheColdWorld.Utils.WebSocket;

namespace TheColdWorld.Utils.Test;

public class WebsocketHelperReceiveTests : IAsyncLifetime
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

    private async Task<(ClientWebSocket Client, System.Net.WebSockets.WebSocket Server)> ConnectRawAsync()
    {
        var client = new ClientWebSocket();
        await client.ConnectAsync(_host.RawUri, CancellationToken.None);
        System.Net.WebSockets.WebSocket server = await _host.RawServerSocket.Task.WaitAsync(TimeSpan.FromSeconds(10));
        return (client, server);
    }

    private static async Task SendAsync(
        ClientWebSocket client,
        byte[] data,
        WebSocketMessageType type = WebSocketMessageType.Binary,
        bool endOfMessage = true)
    {
        await client.SendAsync(data, type, endOfMessage, CancellationToken.None);
    }

    private void Cleanup(ClientWebSocket client, System.Net.WebSockets.WebSocket server)
    {
        _host.RawDone.TrySetResult();
        try { server.Dispose(); } catch (ObjectDisposedException) { }
        try { client.Dispose(); } catch (ObjectDisposedException) { }
    }

    [Fact]
    public async Task ReceiveAsync_ValidPacket_ReturnsSuccessOutcome()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            var packet = new TestPacket(
                PacketBindSide.ServerBind,
                new("test:c2s"),
                new() { ["message"] = "hello" });

            await SendAsync(client, WebsocketHelper.BuildPacket(packet));

            ReceiveOutcome outcome = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            Assert.True(outcome.IsSuccess);
            Assert.True(outcome.TryParsePacket(out Identifier? id, out JsonObject? data));
            Assert.Equal("test:c2s", id!.ToString());
            TestPacket parsed = new(data!);
            Assert.Equal("hello", (string?)parsed.Data["message"]?.AsValue());
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_FragmentedPacket_ReturnsSuccessOutcome()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            var packet = new TestPacket(
                PacketBindSide.ServerBind,
                new("test:c2s"),
                new() { ["message"] = "fragmented" });

            byte[] bytes = WebsocketHelper.BuildPacket(packet);
            int half = bytes.Length / 2;

            await SendAsync(client, bytes[..half], WebSocketMessageType.Binary, endOfMessage: false);
            await SendAsync(client, bytes[half..], WebSocketMessageType.Binary, endOfMessage: true);

            ReceiveOutcome outcome = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            Assert.True(outcome.IsSuccess);
            Assert.True(outcome.TryParsePacket(out _, out JsonObject? data));
            TestPacket parsed = new(data!);
            Assert.Equal("fragmented", (string?)parsed.Data["message"]?.AsValue());
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_InvalidJson_ReturnsInvalidOutcome()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            await SendAsync(client, Encoding.UTF8.GetBytes("{ this is not json"));

            ReceiveOutcome outcome = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            ReceiveOutcome.InvalidOutcome? invalid = outcome.AsInvalidOutcome();
            Assert.NotNull(invalid);
            Assert.IsType<InvalidDataException>(invalid!.Exception);
            Assert.Contains("not valid JSON", invalid.Message);
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_NonObjectJson_ReturnsInvalidOutcome()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            await SendAsync(client, Encoding.UTF8.GetBytes("[1,2,3]"));

            ReceiveOutcome outcome = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            ReceiveOutcome.InvalidOutcome? invalid = outcome.AsInvalidOutcome();
            Assert.NotNull(invalid);
            Assert.IsType<InvalidDataException>(invalid!.Exception);
            Assert.Contains("not an object", invalid.Message);
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_EmptyBinary_ReturnsInvalidOutcome()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            await SendAsync(client, Array.Empty<byte>());

            ReceiveOutcome outcome = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            ReceiveOutcome.InvalidOutcome? invalid = outcome.AsInvalidOutcome();
            Assert.NotNull(invalid);
            Assert.Contains("empty binary", invalid!.Message);
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_TextMessage_ReturnsInvalidOutcome_AndConsumesWholeMessage()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            await SendAsync(client, Encoding.UTF8.GetBytes("hello"), WebSocketMessageType.Text, endOfMessage: false);
            await SendAsync(client, Encoding.UTF8.GetBytes(" world"), WebSocketMessageType.Text, endOfMessage: true);

            ReceiveOutcome outcome = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            ReceiveOutcome.InvalidOutcome? invalid = outcome.AsInvalidOutcome();
            Assert.NotNull(invalid);
            Assert.Contains("non-binary", invalid!.Message);

            // 文本消息应被完整消费，下一条合法 binary packet 仍然能正常收到。
            var packet = new TestPacket(
                PacketBindSide.ServerBind,
                new("test:c2s"),
                new() { ["message"] = "after-text" });

            await SendAsync(client, WebsocketHelper.BuildPacket(packet));

            ReceiveOutcome next = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            Assert.True(next.IsSuccess);
            Assert.True(next.TryParsePacket(out _, out JsonObject? data));
            TestPacket parsed = new(data!);
            Assert.Equal("after-text", (string?)parsed.Data["message"]?.AsValue());
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_NormalClose_ReturnsClosedOutcome()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            await client.CloseOutputAsync(
                WebSocketCloseStatus.NormalClosure,
                "bye",
                CancellationToken.None);

            ReceiveOutcome outcome = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            ReceiveOutcome.ClosedOutcome? closed = outcome.AsClosedOutcome();
            Assert.NotNull(closed);
            Assert.Equal(WebSocketCloseStatus.NormalClosure, closed!.CloseStatus);
            Assert.Equal("bye", closed.CloseStatusDescription);
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_AlreadyClosed_ReturnsClosedOutcome()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            await client.CloseOutputAsync(
                WebSocketCloseStatus.NormalClosure,
                "bye",
                CancellationToken.None);

            ReceiveOutcome first = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));
            Assert.NotNull(first.AsClosedOutcome());

            ReceiveOutcome second = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));
            Assert.NotNull(second.AsClosedOutcome());
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_Abort_ReturnsAbortedOutcome()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            client.Abort();

            ReceiveOutcome outcome = await WebsocketHelper
                .ReceiveAsync(server, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10));

            ReceiveOutcome.AbortedOutcome? aborted = outcome.AsAbortedOutcome();
            Assert.NotNull(aborted);
            Assert.NotNull(aborted!.Exception);
            Assert.False(string.IsNullOrWhiteSpace(aborted.Exception.Message));
        }
        finally
        {
            Cleanup(client, server);
        }
    }

    [Fact]
    public async Task ReceiveAsync_Cancellation_ThrowsOperationCanceledException()
    {
        (ClientWebSocket client, System.Net.WebSockets.WebSocket server) = await ConnectRawAsync();
        try
        {
            using var cts = new CancellationTokenSource();
            Task<ReceiveOutcome> task = WebsocketHelper.ReceiveAsync(server, cts.Token);

            await Task.Delay(100);
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        }
        finally
        {
            Cleanup(client, server);
        }
    }
}
