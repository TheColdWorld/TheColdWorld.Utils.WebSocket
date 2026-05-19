using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using TheColdWorld.Utils;
using TheColdWorld.Utils.socket;
using TheColdWorld.Utils.WebSocket;
using WebSocket_ = System.Net.WebSockets.WebSocket;
namespace TheColdWorld.Utils.WebSocket;

public class WebSocketServerController
{
    private static readonly Lazy<WebSocketServerController> _instance = new(() => new());
    public static WebSocketServerController Instance => _instance.Value;
    private WebSocketServerController() { }
    private readonly ConcurrentDictionary<WebSocket_, byte?> _clients = new();
    public event Func<JsonObject, Identifier, SendToRemoteAsync, CancellationToken, Task>? OnPacketAccept;

    public async Task ProcessClientAsync(WebSocket_ client, CancellationToken token)
    {
        _clients.TryAdd(client, null);
        Byte[] buffer = new byte[4096];
        try
        {
            while (client.State == WebSocketState.Open && !token.IsCancellationRequested)
            {
                var result = await WebSocketConnection.ReceiveAsync(client, token);
                if (result == null) break;

                if (result.ContainsKey("data") && result.ContainsKey("id") &&
                    result["data"] is JsonObject data && result["id"] is JsonValue jv)
                {
                    Identifier id = new(jv.ToString());
                    try
                    {
                        if (OnPacketAccept != null)
                        {
                            async Task sendAsync(IPacket packet, Boolean flag, CancellationToken ct)
                            {
                                if (packet.PacketBindSide != PacketBindSide.ClientBind)
                                    throw new ArgumentException("...");
                                var bytes = Encoding.UTF8.GetBytes(packet.Write().ToJsonString());
                                await client.SendAsync(bytes, WebSocketMessageType.Binary, true, ct);
                            }
                            await OnPacketAccept(data, id, sendAsync, token);
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError(ex);
                    }
                }
            }
        }
        catch (WebSocketException ex) when (ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
        {
        }
        catch (Exception ex)
        {
            LogError(ex);
        }
        finally
        {
            _clients.TryRemove(client, out _);
            if (client.State is not WebSocketState.Closed and not WebSocketState.Aborted)
            {
                await client.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
            }
            client.Dispose();
        }
    }

    public async Task BroadCastAsync(IPacket packet, CancellationToken token = default)
    {
        JsonObject packetObj = new()
        {
            ["id"] = packet.Identifier.ToString(),
            ["data"] = packet.Write()
        };
        byte[] data = Encoding.UTF8.GetBytes(packetObj.ToJsonString());
        var tasks = _clients.Keys.Select(async client =>
        {
            try
            {
                if (client.State == WebSocketState.Open)
                    await client.SendAsync(data, WebSocketMessageType.Binary, true, token);
            }
            catch (Exception ex)
            {
                _clients.TryRemove(client, out _);
                LogError(ex);
            }
        });
        await Task.WhenAll(tasks);
    }

    private void LogError(Exception ex) {
#if DEBUG
        Console.WriteLine("[Debug](Exception):"+ex.ToString());
#endif

    }
}

public delegate Task SendToRemoteAsync(IPacket packet, bool flag, CancellationToken token);