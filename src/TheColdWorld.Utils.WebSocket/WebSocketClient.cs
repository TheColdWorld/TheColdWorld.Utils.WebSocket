using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Net;
using System.Net.WebSockets;
using System.Runtime.Versioning;
using System.Text;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;

namespace TheColdWorld.Utils.WebSocket;

public sealed class WebSocketClient:IAsyncDisposable
{
    public WebSocketClient(Uri uri,  CancellationToken cancellationToken = default)
    {
        if (!uri.IsAbsoluteUri)throw new ArgumentException("Uri must be absolute", nameof(uri));
        if (uri.Scheme != Uri.UriSchemeWs && uri.Scheme != Uri.UriSchemeWss) throw new ArgumentException("Uri must begin with \'ws://\' or \'wss://\'",nameof(uri));
        _connection = new();
        _remoteUri= uri;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    }
    public Task ConnectAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connection.CloseStatus is not null) throw new InvalidOperationException($"Websocket connection is closed because {_connection.CloseStatusDescription}({_connection.CloseStatus.Value})");
        if (_connectTask != null) return _connectTask; 
        _connectTask= _connection.ConnectAsync(_remoteUri, _cancellationTokenSource.Token);
        return _connectTask;
    }
    public async ValueTask<(Identifier,JsonObject)> ReceiveAsync(CancellationToken token=default)
    {
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
            var packet = await WebsocketHelper.ReceiveAsync(_connection, source.Token);
            if (WebsocketHelper.TryParsePacket(packet, out var id, out var data))
            {
                return (id, data);
            }
            else throw new InvalidDataException("Cannot decode packet:received binary's format is wrong");
        }
        finally { _silm.Release(); }
    }
    public async Task SendAsync(IPacket packet,CancellationToken token=default)
    {
        if (packet.PacketBindSide != PacketBindSide.ServerBind) throw new ArgumentException($"packet must be {PacketBindSide.ServerBind},but given {PacketBindSide.ClientBind}",nameof(packet));
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
            await _connection.SendAsync(data,WebSocketMessageType.Binary,true,source.Token);
        }
        finally { _silm.Release(); }
    }
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        await _silm.WaitAsync();
        try
        {
            if(_disposed) return;
            _disposed = true;
            try
            {
                await _connection.CloseAsync(WebSocketCloseStatus.NormalClosure, "Client Disposing", _cancellationTokenSource.Token);
            }
            catch (ObjectDisposedException) { }
            await _cancellationTokenSource.CancelAsync();
            _connection.Dispose();
            _cancellationTokenSource.Dispose();
            GC.SuppressFinalize(this);
        }
        finally { _silm.Release();_silm.Dispose(); }
    }
    Task? _connectTask=null;
    readonly ClientWebSocket _connection;
    readonly Uri _remoteUri;
    readonly CancellationTokenSource _cancellationTokenSource;
    readonly SemaphoreSlim _silm = new(1, 1);
    bool _disposed = false;
}