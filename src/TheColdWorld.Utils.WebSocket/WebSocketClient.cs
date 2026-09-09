using System.Net.Sockets;
using System.Net.WebSockets;
using TheColdWorld.Utils.socket;

namespace TheColdWorld.Utils.WebSocket;

public sealed class WebSocketClient : IAsyncDisposable
{
    public WebSocketClient(Uri uri, CancellationToken cancellationToken = default)
    {
        if (!uri.IsAbsoluteUri) throw new ArgumentException("Uri must be absolute", nameof(uri));
        if (uri.Scheme != Uri.UriSchemeWs && uri.Scheme != Uri.UriSchemeWss) throw new ArgumentException("Uri must begin with 'ws://' or 'wss://'", nameof(uri));
        _connection = new();
        _remoteUri = uri;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    }

    public static event Action<Logging.LogLevel, string, Exception?>? OnLogging;

    public Task ConnectAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connection.CloseStatus is not null) throw new InvalidOperationException($"Websocket connection is closed because {_connection.CloseStatusDescription}({_connection.CloseStatus.Value})");
        if (_connectTask != null) return _connectTask;
        _connectTask = _connection.ConnectAsync(_remoteUri, _cancellationTokenSource.Token);
        return _connectTask;
    }


    public async ValueTask<ReceiveOutcome> ReceiveAsync(CancellationToken token = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        CancellationTokenSource source = CancellationTokenSource.CreateLinkedTokenSource(token, _cancellationTokenSource.Token);
        await _silm.WaitAsync(source.Token);
        ObjectDisposedException.ThrowIf(_disposed, this);
        try
        {
            if (_connection.CloseStatus is not null)
            {
                return LogIfFailed(new ReceiveOutcome.ClosedOutcome(
                    _connection.CloseStatusDescription ?? _connection.CloseStatus.Value.ToString(),
                    _connection.CloseStatus,
                    _connection.CloseStatusDescription));
            }

            if (_connection.State == WebSocketState.None) await ConnectAsync();

            while (_connection.State != WebSocketState.Open)
            {
                await Task.Yield();

                if (_connection.CloseStatus is not null)
                {
                    return LogIfFailed(new ReceiveOutcome.ClosedOutcome(
                        _connection.CloseStatusDescription ?? _connection.CloseStatus.Value.ToString(),
                        _connection.CloseStatus,
                        _connection.CloseStatusDescription));
                }

                if (_connection.State == WebSocketState.Aborted)
                {
                    return LogIfFailed(new ReceiveOutcome.AbortedOutcome(
                        new WebSocketException(
                            WebSocketError.ConnectionClosedPrematurely,
                            "WebSocket connection was aborted.")));
                }

                if (_connection.State is WebSocketState.Closed
                    or WebSocketState.CloseReceived
                    or WebSocketState.CloseSent)
                {
                    return LogIfFailed(new ReceiveOutcome.ClosedOutcome(
                        $"WebSocket is in state {_connection.State}."));
                }
            }

            ReceiveOutcome outcome = await WebsocketHelper.ReceiveAsync(_connection, source.Token);
            return LogIfFailed(outcome);
        }
        finally { _silm.Release(); }
    }

    public async Task SendAsync(IPacket packet, CancellationToken token = default)
    {
        if (packet.PacketBindSide != PacketBindSide.ServerBind) throw new ArgumentException($"packet must be {PacketBindSide.ServerBind},but given {PacketBindSide.ClientBind}", nameof(packet));
        ObjectDisposedException.ThrowIf(_disposed, this);
        CancellationTokenSource source = CancellationTokenSource.CreateLinkedTokenSource(token, _cancellationTokenSource.Token);
        await _silm.WaitAsync(source.Token);
        ObjectDisposedException.ThrowIf(_disposed, this);
        try
        {
            if (_connection.CloseStatus is not null) throw new InvalidOperationException($"Websocket connection is closed because {_connection.CloseStatusDescription}({_connection.CloseStatus.Value})");
            if (_connection.State == WebSocketState.None) await ConnectAsync();
            while (_connection.State != WebSocketState.Open)
            {
                await Task.Yield();
                if (_connection.CloseStatus is not null) throw new InvalidOperationException($"Websocket connection is closed because {_connection.CloseStatusDescription}({_connection.CloseStatus.Value})");
            }
            var data = WebsocketHelper.BuildPacket(packet);
            await _connection.SendAsync(data, WebSocketMessageType.Binary, true, source.Token);
        }
        finally { _silm.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        await _silm.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;
            try
            {
                if (_connection.State is WebSocketState.Open or WebSocketState.CloseReceived)
                {
                    await _connection.CloseOutputAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "Client Disposing",
                        CancellationToken.None);
                }
            }
            catch (Exception e) when (IsTransportClosed(e))
            {
                OnLogging?.Invoke(Logging.LogLevel.Warning, e.Message, e);
            }
            catch (OperationCanceledException) { }

            await _cancellationTokenSource.CancelAsync();
            _connection.Dispose();
            _cancellationTokenSource.Dispose();
            GC.SuppressFinalize(this);
        }
        finally { _silm.Release(); _silm.Dispose(); }
    }

    private static ReceiveOutcome LogIfFailed(ReceiveOutcome outcome)
    {
        if (outcome.AsAbortedOutcome() is { } aborted)
            OnLogging?.Invoke(Logging.LogLevel.Warning, aborted.Exception.Message, aborted.Exception);
        else if (outcome.AsInvalidOutcome() is { } invalid)
            OnLogging?.Invoke(Logging.LogLevel.Warning, invalid.Exception.Message, invalid.Exception);

        return outcome;
    }

    private static bool IsTransportClosed(Exception e)
        => e is WebSocketException
            or IOException
            or SocketException
            or ObjectDisposedException;

    Task? _connectTask = null;
    readonly ClientWebSocket _connection;
    readonly Uri _remoteUri;
    readonly CancellationTokenSource _cancellationTokenSource;
    readonly SemaphoreSlim _silm = new(1, 1);
    bool _disposed = false;
}
