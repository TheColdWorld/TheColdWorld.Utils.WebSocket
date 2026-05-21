using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;
using TheColdWorld.Utils.Thread;

namespace TheColdWorld.Utils.WebSocket;

public class WebSocketClient : IDisposable
{
    public WebSocketClient(Uri uri, Action<JsonObject, Identifier, SendToRemote> packetAccept, string threadNamePrefix = "TheColdWorld-TcpClient-ThreadPool", CancellationToken cancellationToken = default)
    {
        ClientWebSocket ws = new ();
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        asyncService = new(threadNamePrefix, threadCount: 5);
        Task task= ws.ConnectAsync(uri, _cts.Token);
        try
        {
            Task.WaitAll(task);
        }
        catch (AggregateException ex)
        {
            if (ex.InnerException is not null) throw ex.InnerException;
            throw;
        }
        catch { throw; }
        _connection = new(ws,asyncService,packetAccept,_cts.Token,PacketBindSide.ServerBind);
    }
    WebSocketConnection _connection;
    internal readonly AsyncService asyncService;
    readonly CancellationTokenSource _cts;
    internal volatile bool _disposed = false;
    public void Send<TPacket>(TPacket packet, SocketFlags flags = SocketFlags.None) where TPacket : class, IPacket
    {
        if (packet.PacketBindSide != PacketBindSide.ServerBind) throw new ArgumentException("Trying use client to send client bound packet", nameof(packet));
        _connection.Send(packet, flags);
    }
    public Task SendAsync<TPacket>(TPacket packet, SocketFlags flags = SocketFlags.None) where TPacket : class, IPacket =>
        packet.PacketBindSide != PacketBindSide.ServerBind
            ? throw new ArgumentException("Trying use client to send client bound packet", nameof(packet))
            : _connection.SendAsync(packet);
    public void Send(IPacket packet, SocketFlags flags = SocketFlags.None)
    {
        if (packet.PacketBindSide != PacketBindSide.ServerBind) throw new ArgumentException("Trying use client to send client bound packet", nameof(packet));
        _connection.Send(packet, flags);
    }
    public Task SendAsync(IPacket packet, SocketFlags flags = SocketFlags.None) =>
        packet.PacketBindSide != PacketBindSide.ServerBind
            ? throw new ArgumentException("Trying use client to send client bound packet", nameof(packet))
            : _connection.SendAsync(packet);
    public void Dispose()
    {
        if (_disposed) return;
        lock (this)
        {
            if (_disposed) return;
            _disposed = true;
        }
        _cts.Cancel();
        _connection?.Dispose();
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }
}
