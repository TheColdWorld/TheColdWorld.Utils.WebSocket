using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;

namespace TheColdWorld.Utils.WebSocket;

public static class WebsocketHelper
{
    public static byte[] BuildPacket<TPacket>(TPacket packet) where TPacket:IPacket,allows ref struct
    {
        JsonObject packetObj = new()
        {
            ["id"] = packet.Identifier.ToString(),
            ["data"] = packet.Write()
        };
        return Encoding.UTF8.GetBytes(packetObj.ToJsonString());
    }
    public static byte[] BuildPacket(IPacket packet)
    {
        JsonObject packetObj = new()
        {
            ["id"] = packet.Identifier.ToString(),
            ["data"] = packet.Write()
        };
        return Encoding.UTF8.GetBytes(packetObj.ToJsonString());
    }
    public static bool TryParsePacket(JsonObject packet,[NotNullWhen(true)]out Identifier? id,[NotNullWhen(true)]out JsonObject? packetData)
    {
        if (packet.ContainsKey("data") && packet.ContainsKey("id") && packet["data"] is JsonObject data && packet["id"] is JsonValue jv && jv.TryGetValue(out string? pid) && pid is not null)
        {
            try
            {
                id = new(pid);
                packetData=data;
                return true;
            }
            catch (ArgumentException) { id= null;packetData = null;  return false; }
            catch{ throw; }
        }
        {
            id = null;
            packetData = null;
            return false;
        }
    }
    public static async Task<JsonObject> ReceiveAsync(System.Net.WebSockets.WebSocket ws, CancellationToken cancellation = default)
    {
        if (ws.CloseStatus is not null) throw new InvalidOperationException($"Websocket connection is closed because {ws.CloseStatusDescription}({ws.CloseStatus.Value})");
        using MemoryStream ms = new();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64);
        try
        {
            WebSocketReceiveResult result;
            do
            {
                result = await ws.ReceiveAsync(buffer, cancellation);
                if (result.MessageType != WebSocketMessageType.Binary) continue;
                if (result.CloseStatus != null)
                {
                    throw new InvalidOperationException($"Websocket connection is closed because {result.CloseStatusDescription}({result.CloseStatus.Value})");
                }
                await ms.WriteAsync(buffer.AsMemory(0, result.Count), cancellation);
                await Task.Yield();
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
