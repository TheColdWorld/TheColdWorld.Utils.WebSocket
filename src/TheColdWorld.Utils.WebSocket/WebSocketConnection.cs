using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using TheColdWorld.Utils.socket;
using TheColdWorld.Utils.Thread;

namespace TheColdWorld.Utils.WebSocket;

internal sealed class WebSocketConnection :IDisposable
{
    internal const int BUFFER_LENGTH = 1024 ;//1KB
    public WebSocketConnection(System.Net.WebSockets.WebSocket connection, AsyncService asyncService, Action<JsonObject, Identifier, SendToRemote> packetAccept, CancellationToken token, PacketBindSide remoteSide)
    {
        _connection = connection;
        this._cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        this.asyncService = asyncService;
        this.packetAccept = packetAccept;
        this.remoteSide = remoteSide;
        SendQueue = Channel.CreateUnbounded<Task>(); ;
        RecvTask = asyncService.Run(RecvLoop, _cts.Token);
        SendTask = asyncService.Run(SendLoop, _cts.Token);
    }
    private async Task RecvLoop()
    {
        while (_connection.State == WebSocketState.Connecting) await Task.Delay(1);
        while (_connection.State == WebSocketState.Open && stable && !_cts.IsCancellationRequested)
        {
            try
            {
                JsonObject result = await ReceiveAsync(_connection, _cts.Token);
                if (result.ContainsKey("data") && result.ContainsKey("id") && result["data"] is JsonObject data && result["id"] is JsonValue jv)
                {
                    Identifier id = new(jv.ToString());
                    _ = asyncService.Run(() =>
                    {
                        try
                        {
                            this.packetAccept.Invoke(data, id, (packet, flag, token) =>
                            {
                                if (packet.PacketBindSide != remoteSide) throw new ArgumentException("Trying use server to send server bound packet", nameof(packet));
                                Send(packet, flag, token);
                            });
                        }
                        catch (Exception)
                        {
                        }
                    });
                }
            }
            catch (WebSocketException)
            {
                _ = asyncService.Run(Dispose);
                return;
            }
            catch (Exception) { }
        }
    }
    private async Task SendLoop()
    {

        while (_connection.State == WebSocketState.Connecting) await Task.Delay(1);

        while (_connection.State == WebSocketState.Open && stable && !_cts.IsCancellationRequested)
        {
            if(await SendQueue.Reader.WaitToReadAsync())
            {
                try
                {
                    Task task = await SendQueue.Reader.ReadAsync(_cts.Token);
                    _= asyncService.Start(task);
                    await task;
                }
                catch (OperationCanceledException) { break; }
                catch (Exception){}
            }
        }
    }
    internal void Send(IPacket packet, SocketFlags flags = SocketFlags.None, CancellationToken cancellationToken = default) 
    {
        SendQueue.Writer.TryWrite(new(async () =>
        {
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
            JsonObject packetObj = new()
            {
                ["id"] = packet.Identifier.ToString(),
                ["data"] = packet.Write()
            };
            await _connection.SendAsync(Encoding.UTF8.GetBytes(packetObj.ToJsonString()), WebSocketMessageType.Binary, true, cts.Token);
        }));
    }
    internal void Send<TPacket>(TPacket packet, SocketFlags flags = SocketFlags.None, CancellationToken cancellationToken = default) where TPacket : class, IPacket
    {
        SendQueue.Writer.TryWrite(new(async () =>
        {
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
            JsonObject packetObj = new()
            {
                ["id"] = packet.Identifier.ToString(),
                ["data"] = packet.Write()
            };
            await _connection.SendAsync(Encoding.UTF8.GetBytes(packetObj.ToJsonString()), WebSocketMessageType.Binary, true, cts.Token);
        }));
    }
    internal Task SendAsync<TPacket>(TPacket packet, CancellationToken cancellationToken = default) where TPacket : class, IPacket
    {
        Task task = new(async () =>
        {
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
            JsonObject packetObj = new()
            {
                ["id"] = packet.Identifier.ToString(),
                ["data"] = packet.Write()
            };
            await _connection.SendAsync(Encoding.UTF8.GetBytes(packetObj.ToJsonString()), WebSocketMessageType.Binary, true, cts.Token);
        });
        SendQueue.Writer.TryWrite(task);
        return task;
    }
    internal Task SendAsync(IPacket packet, CancellationToken cancellationToken = default)
    {
        Task task = new(async () =>
        {
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
            JsonObject packetObj = new()
            {
                ["id"] = packet.Identifier.ToString(),
                ["data"] = packet.Write()
            };
            await _connection.SendAsync(Encoding.UTF8.GetBytes(packetObj.ToJsonString()), WebSocketMessageType.Binary, true, cts.Token);
        });
        SendQueue.Writer.TryWrite(task);
        return task;
    }
    public void Dispose()
    {
        if (_disposed) return;
        using(_lock.EnterScope())
        {
            if(_disposed ) return;
            _disposed = true;
            stable = false;
        }
        _cts.Cancel();
        try
        {
            RecvTask.Wait(5000);
            SendTask.Wait(5000);
        }
        catch (AggregateException) { }
        try
        {
            _ = _connection.CloseAsync(WebSocketCloseStatus.NormalClosure, null,default);
        }
        catch { }
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }
    private readonly System.Net.WebSockets.WebSocket _connection;
    internal PacketBindSide remoteSide;
    internal void ThrowIfDisposed() { if (!_disposed) return; throw new ObjectDisposedException(nameof(_connection)); }
    internal AsyncService asyncService;
    internal Channel<Task> SendQueue;
    internal Task RecvTask;
    internal Task SendTask;
    internal readonly CancellationTokenSource _cts;
    private volatile bool _disposed;
    private volatile bool stable = true;
    private readonly Lock _lock = new();
    private readonly Action<JsonObject, Identifier, SendToRemote> packetAccept;
    ~WebSocketConnection() => this.Dispose();
    internal static async Task<JsonObject> ReceiveAsync(System.Net.WebSockets.WebSocket ws, CancellationToken cancellation = default)
    {
        if(ws.CloseStatus  is not null) throw new InvalidOperationException();
        ArrayPool<byte> pool = ArrayPool<byte>.Shared;
        using MemoryStream ms = new MemoryStream();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(BUFFER_LENGTH);
        try
        {
            WebSocketReceiveResult result;
            do
            {
                result = await ws.ReceiveAsync(buffer, cancellation);
                if (result.MessageType != WebSocketMessageType.Binary)
                    return new JsonObject();
                if (result.CloseStatus != null)
                    return [];

                ms.Write(buffer, 0, result.Count);
            }
            while (!result.EndOfMessage && !cancellation.IsCancellationRequested);

            ms.Position = 0;
            var jsonNode = await JsonNode.ParseAsync(ms, cancellationToken: cancellation);
            return jsonNode as JsonObject ?? [];
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
