using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Runtime.Versioning;
using System.Text;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;

namespace TheColdWorld.Utils.WebSocket;

[UnsupportedOSPlatform("Browser","This is a server-side class ,you shouldn't use it on client on Browser")]
public sealed class WebSocketServerControlUnit : IAsyncDisposable
{
    public WebSocketServerControlUnit() { }
    public static event Action<Logging.LogLevel, string,Exception?>? OnLogging;
    /// <summary>
    /// handle a <see cref="System.Net.WebSockets.WebSocket"/>'s all packet and auto invoke <see cref="System.Net.WebSockets.WebSocket.CloseAsync"/> and <see cref="System.Net.WebSockets.WebSocket.Dispose"/> 
    /// </summary>
    /// <param name="client"> client to handle</param>
    /// <seealso cref="OnPacketAccepted"/> when a vaild packet received from any client used in this method
    public async Task HandleClient(System.Net.WebSockets.WebSocket client,CancellationToken token=default)
    {
        ArgumentNullException.ThrowIfNull(client, nameof(client));
        ObjectDisposedException.ThrowIf(_disposed, this);
        Task? task=null;
        try
        {
            await AddClientAsync(client, token);
            task= HandleClientInternal(client, token);
            await AddTaskAsync(task,token);
            await task;
        }
        catch (TaskCanceledException) { }
        catch (OperationCanceledException) { }
        catch (Exception e) 
        {
            OnLogging?.Invoke(Logging.LogLevel.Error,"Exception occored on receiving a packet",e) ;
            await client.CloseAsync(WebSocketCloseStatus.InternalServerError,e.ToString(),token);
            throw;
        }
        finally
        {
            await RemoveClientAsync(client, default);
            if(task is not null) await RemoveTaskAsync(task,default);
            if (client.CloseStatus == null)  await client.CloseAsync(WebSocketCloseStatus.NormalClosure, _disposed?"Server Closed":null, default); 
            client.Dispose(); 
        }
    }
    private async Task HandleClientInternal([NotNull]System.Net.WebSockets.WebSocket client, CancellationToken token)
    {
        bool running = true;
        while (!_disposed && !token.IsCancellationRequested && client.CloseStatus == null && running)
        {
            if (WebsocketHelper.TryParsePacket(await WebsocketHelper.ReceiveAsync(client, token), out var id, out var data))
            {
                var handler = OnPacketAccepted;
                if (handler != null) await handler.Invoke(id, data, async (p, t) =>
                {
                    if (p.PacketBindSide != PacketBindSide.ClientBind) throw new ArgumentException($"packet must be {PacketBindSide.ClientBind},but given {PacketBindSide.ServerBind}", nameof(p));
                    byte[] _data = WebsocketHelper.BuildPacket(p);
                    await client.SendAsync(_data, WebSocketMessageType.Binary, true, t);
                }, () => running = false, token);
                else OnLogging?.Invoke(Logging.LogLevel.Warning, "client received a packet but handler is null,which means a packet was ignored", null);
            }
        }
    }
    public async Task BroadCastAsync(IPacket packet,CancellationToken token=default)
    {
        if (packet.PacketBindSide != PacketBindSide.ClientBind) throw new ArgumentException($"packet must be {PacketBindSide.ClientBind},but given {PacketBindSide.ServerBind}", nameof(packet));
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _silm.WaitAsync();
        byte[] _data = WebsocketHelper.BuildPacket(packet);
        try
        {
            _clients.ForEach(async s => await s.SendAsync(_data, WebSocketMessageType.Binary, true, token));
        }
        finally { _silm.Release(); }
    }
    readonly List<System.Net.WebSockets.WebSocket>_clients = [];
    readonly SemaphoreSlim _silm = new(1,1);
    private bool _disposed= false;
    private readonly HashSet<Task> _tasks = [];
    public event HandlePacketAsync? OnPacketAccepted;
    private async Task AddClientAsync(System.Net.WebSockets.WebSocket client,CancellationToken token)
    {
        await _silm.WaitAsync(token);
        try
        {
            _clients.Add(client);
        }
        finally { _silm.Release();}
    }
    private async Task RemoveClientAsync(System.Net.WebSockets.WebSocket client,CancellationToken token)
    {
        await _silm.WaitAsync(token);
        try
        {
            _clients.Remove(client);
        }
        finally { _silm.Release(); }
    }

    private async Task AddTaskAsync(Task task,CancellationToken token)
    {
        await _silm.WaitAsync(token);
        try
        {
            _tasks.Add(task);
        }finally { _silm.Release(); }
    }
    private async Task RemoveTaskAsync(Task task, CancellationToken token)
    {
        await _silm.WaitAsync(token);
        try
        {
            _tasks.Remove(task);
        }
        finally { _silm.Release(); }
    }
    public delegate Task HandlePacketAsync(Identifier id,JsonObject dataObj,SendToClientAsync sendToClient,Action stopAction,CancellationToken token);
    public delegate Task SendToClientAsync(IPacket packet,CancellationToken token);
    public async ValueTask DisposeAsync()
    {
        if(_disposed) return;
        _disposed = true;
        await _silm.WaitAsync();
        try
        {
            if (_tasks.Count > 0) await Task.WhenAll(_tasks).ConfigureAwait(false);
            OnPacketAccepted = null;
            _clients.Clear();
            GC.SuppressFinalize(this);
        }
        finally { _silm.Release();_silm.Dispose(); }
    }
    private static readonly Lazy<WebSocketServerControlUnit> _instance = new(() => new());
    public static readonly WebSocketServerControlUnit Default = _instance.Value;
}
