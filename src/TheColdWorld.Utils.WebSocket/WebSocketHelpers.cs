using System.Buffers;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.Thread;

namespace TheColdWorld.Utils.WebSocket;

public static class WebSocketHelpers
{
    public static (Identifier,JsonObject)? ParcePacket(JsonObject result)
    {
        if (result.ContainsKey("data") && result.ContainsKey("id") && result["data"] is JsonObject data && result["id"] is JsonValue jv)
        {
            Identifier id = new(jv.ToString());
            return (id, data);
        }
        else return null;
    }
    public static async Task<JsonObject> ReceiveAsync(System.Net.WebSockets.WebSocket ws, CancellationToken cancellation = default)
    {
        if (ws.CloseStatus is not null) throw new InvalidOperationException();
        ArrayPool<byte> pool = ArrayPool<byte>.Shared;
        using MemoryStream ms = new MemoryStream();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(WebSocketConnection.BUFFER_LENGTH);
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