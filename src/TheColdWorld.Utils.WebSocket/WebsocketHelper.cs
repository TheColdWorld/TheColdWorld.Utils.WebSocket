using System.Buffers;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;

namespace TheColdWorld.Utils.WebSocket;

public static class WebsocketHelper
{
    public static byte[] BuildPacket<TPacket>(TPacket packet) where TPacket : IPacket, allows ref struct
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

    public static async Task<ReceiveOutcome> ReceiveAsync(
        System.Net.WebSockets.WebSocket ws,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(ws);
        cancellation.ThrowIfCancellationRequested();

        if (ws.CloseStatus is not null)
        {
            return new ReceiveOutcome.ClosedOutcome(
                ws.CloseStatusDescription ?? ws.CloseStatus.Value.ToString(),
                ws.CloseStatus,
                ws.CloseStatusDescription);
        }

        if (ws.State is not WebSocketState.Open)
        {
            return ws.State switch
            {
                WebSocketState.Aborted => new ReceiveOutcome.AbortedOutcome(
                    new WebSocketException(
                        WebSocketError.ConnectionClosedPrematurely,
                        $"WebSocket is in state {ws.State}.")),
                WebSocketState.CloseReceived or WebSocketState.CloseSent or WebSocketState.Closed
                    => new ReceiveOutcome.ClosedOutcome($"WebSocket is in state {ws.State}."),
                _ => new ReceiveOutcome.AbortedOutcome(
                    new InvalidOperationException(
                        $"WebSocket is not connected (state: {ws.State})."))
            };
        }

        using MemoryStream ms = new();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(1024);
        try
        {
            WebSocketReceiveResult result;
            do
            {
                try
                {
                    result = await ws.ReceiveAsync(buffer, cancellation);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception e) when (IsTransportClosed(e))
                {
                    return new ReceiveOutcome.AbortedOutcome(e);
                }

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return new ReceiveOutcome.ClosedOutcome(
                        result.CloseStatusDescription
                            ?? result.CloseStatus?.ToString()
                            ?? "WebSocket closed by remote.",
                        result.CloseStatus,
                        result.CloseStatusDescription);
                }

                if (result.MessageType != WebSocketMessageType.Binary)
                {
                    while (!result.EndOfMessage)
                    {
                        try
                        {
                            result = await ws.ReceiveAsync(buffer, cancellation);
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception e) when (IsTransportClosed(e))
                        {
                            return new ReceiveOutcome.AbortedOutcome(e);
                        }

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            return new ReceiveOutcome.ClosedOutcome(
                                result.CloseStatusDescription
                                    ?? result.CloseStatus?.ToString()
                                    ?? "WebSocket closed by remote.",
                                result.CloseStatus,
                                result.CloseStatusDescription);
                        }
                    }

                    string message = $"Received a non-binary WebSocket message: {result.MessageType}.";
                    return new ReceiveOutcome.InvalidOutcome(message, new InvalidDataException(message));
                }

                await ms.WriteAsync(buffer.AsMemory(0, result.Count), cancellation);
            }
            while (!result.EndOfMessage);

            if (ms.Length == 0)
            {
                const string message = "Received an empty binary message.";
                return new ReceiveOutcome.InvalidOutcome(message, new InvalidDataException(message));
            }

            ms.Position = 0;
            JsonNode? node;
            try
            {
                node = await JsonNode.ParseAsync(ms, cancellationToken: cancellation);
            }
            catch (JsonException e)
            {
                string message = $"Received message is not valid JSON: {e.Message}";
                return new ReceiveOutcome.InvalidOutcome(message, new InvalidDataException(message, e));
            }

            if (node is not JsonObject obj)
            {
                const string message = "Received JSON is not an object.";
                return new ReceiveOutcome.InvalidOutcome(message, new InvalidDataException(message));
            }

            return new ReceiveOutcome.SuccessOutcome(obj);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static bool IsTransportClosed(Exception e)
        => e is WebSocketException
            or IOException
            or SocketException
            or ObjectDisposedException;
}
