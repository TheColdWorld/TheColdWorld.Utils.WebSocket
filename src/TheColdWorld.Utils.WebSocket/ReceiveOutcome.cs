using System.Diagnostics.CodeAnalysis;
using System.Net.WebSockets;
using System.Runtime.ExceptionServices;
using System.Text.Json.Nodes;

namespace TheColdWorld.Utils.WebSocket;
/// <summary>
/// Result of websocket receive
/// </summary>
public abstract class ReceiveOutcome
{
    public abstract bool IsSuccess { get; }

    public abstract JsonObject GetOrThrow();

    public abstract ReceiveOutcome IfSuccess(Action<JsonObject> action);

    public abstract ReceiveOutcome IfFailed(Action<string, Exception?> onException);

    public bool TryParsePacket(
        [NotNullWhen(true)] out Identifier? id,
        [NotNullWhen(true)] out JsonObject? packetData)
    {
        if (!IsSuccess)
        {
            id = null;
            packetData = null;
            return false;
        }

        return TryParsePacketCore(GetOrThrow(), out id, out packetData);
    }

    public SuccessOutcome? AsSuccessOutcome() => this as SuccessOutcome;

    public ClosedOutcome? AsClosedOutcome() => this as ClosedOutcome;

    public AbortedOutcome? AsAbortedOutcome() => this as AbortedOutcome;

    public InvalidOutcome? AsInvalidOutcome() => this as InvalidOutcome;

    private static bool TryParsePacketCore(
        JsonObject packet,
        [NotNullWhen(true)] out Identifier? id,
        [NotNullWhen(true)] out JsonObject? packetData)
    {
        if (packet.ContainsKey("data") && packet.ContainsKey("id")
            && packet["data"] is JsonObject data
            && packet["id"] is JsonValue jv
            && jv.TryGetValue(out string? pid) && pid is not null)
        {
            string[] parts = pid.Split(':', 2);
            if (parts.Length == 2
                && !string.IsNullOrWhiteSpace(parts[0])
                && !string.IsNullOrWhiteSpace(parts[1]))
            {
                id = new(pid);
                packetData = data;
                return true;
            }
        }

        id = null;
        packetData = null;
        return false;
    }

    public sealed class SuccessOutcome(JsonObject packet) : ReceiveOutcome
    {
        public JsonObject Packet { get; } = packet ?? throw new ArgumentNullException(nameof(packet));

        public override bool IsSuccess => true;

        public override JsonObject GetOrThrow() => Packet;
        public (Identifier id, JsonObject data) ParsePacket()
        {
            if (!TryParsePacket(out var id, out var data))
                throw new InvalidDataException("Cannot decode packet: received binary's format is wrong");

            return (id, data);
        }

        public override ReceiveOutcome IfSuccess(Action<JsonObject> action)
        {
            ArgumentNullException.ThrowIfNull(action);
            action(Packet);
            return this;
        }

        public override ReceiveOutcome IfFailed(Action<string, Exception?> onException) => this;
    }


    public sealed class ClosedOutcome(
        string message,
        WebSocketCloseStatus? closeStatus = null,
        string? closeStatusDescription = null) : ReceiveOutcome
    {
        public string Message { get; } = message ?? throw new ArgumentNullException(nameof(message));

        public WebSocketCloseStatus? CloseStatus { get; } = closeStatus;

        public string? CloseStatusDescription { get; } = closeStatusDescription;

        public override bool IsSuccess => false;

        public override JsonObject GetOrThrow() => throw new InvalidOperationException(Message);

        public override ReceiveOutcome IfSuccess(Action<JsonObject> action) => this;

        public override ReceiveOutcome IfFailed(Action<string, Exception?> onException)
        {
            ArgumentNullException.ThrowIfNull(onException);
            onException(Message, null);
            return this;
        }
    }


    public sealed class AbortedOutcome(System.Exception exception) : ReceiveOutcome
    {
        public System.Exception Exception { get; } =
            exception ?? throw new ArgumentNullException(nameof(exception));

        public string Message => Exception.Message;

        public override bool IsSuccess => false;

        public override JsonObject GetOrThrow()
        {
            ExceptionDispatchInfo.Capture(Exception).Throw();
            throw new InvalidOperationException("Unreachable");
        }

        public override ReceiveOutcome IfSuccess(Action<JsonObject> action) => this;

        public override ReceiveOutcome IfFailed(Action<string, Exception?> onException)
        {
            ArgumentNullException.ThrowIfNull(onException);
            onException(Message, Exception);
            return this;
        }
    }

    public sealed class InvalidOutcome(string message, System.Exception exception) : ReceiveOutcome
    {
        public string Message { get; } = message ?? throw new ArgumentNullException(nameof(message));

        public System.Exception Exception { get; } =
            exception ?? throw new ArgumentNullException(nameof(exception));

        public override bool IsSuccess => false;

        public override JsonObject GetOrThrow()
        {
            ExceptionDispatchInfo.Capture(Exception).Throw();
            throw new InvalidOperationException("Unreachable");
        }

        public override ReceiveOutcome IfSuccess(Action<JsonObject> action) => this;

        public override ReceiveOutcome IfFailed(Action<string, Exception?> onException)
        {
            ArgumentNullException.ThrowIfNull(onException);
            onException(Message, Exception);
            return this;
        }
    }
}
