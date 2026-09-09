using System.Net.WebSockets;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.WebSocket;

namespace TheColdWorld.Utils.Test;

public class ReceiveOutcomeTests
{
    private static JsonObject MakePacket(string id = "test:c2s", string message = "hello")
        => new()
        {
            ["id"] = id,
            ["data"] = new JsonObject { ["message"] = message }
        };

    [Fact]
    public void SuccessOutcome_ExposesPacketAndFlags()
    {
        JsonObject packet = MakePacket();
        var outcome = new ReceiveOutcome.SuccessOutcome(packet);

        Assert.True(outcome.IsSuccess);
        Assert.Same(packet, outcome.Packet);
        Assert.Same(packet, outcome.GetOrThrow());
        Assert.NotNull(outcome.AsSuccessOutcome());
        Assert.Null(outcome.AsClosedOutcome());
        Assert.Null(outcome.AsAbortedOutcome());
        Assert.Null(outcome.AsInvalidOutcome());
    }

    [Fact]
    public void SuccessOutcome_ParsePacket_ReturnsIdAndData()
    {
        var outcome = new ReceiveOutcome.SuccessOutcome(MakePacket());

        (Identifier id, JsonObject data) = outcome.ParsePacket();

        Assert.Equal("test:c2s", id.ToString());
        Assert.Equal("hello", (string?)data["message"]?.AsValue());
    }

    [Fact]
    public void SuccessOutcome_TryParsePacket_ReturnsTrueAndOutputs()
    {
        var outcome = new ReceiveOutcome.SuccessOutcome(MakePacket());

        Assert.True(outcome.TryParsePacket(out Identifier? id, out JsonObject? data));
        Assert.NotNull(id);
        Assert.NotNull(data);
        Assert.Equal("test:c2s", id!.ToString());
        Assert.Equal("hello", (string?)data!["message"]?.AsValue());
    }

    [Fact]
    public void SuccessOutcome_ParsePacket_Throws_WhenIdMissingColon()
    {
        var outcome = new ReceiveOutcome.SuccessOutcome(MakePacket("bad"));

        Assert.Throws<InvalidDataException>(() => outcome.ParsePacket());
    }

    [Fact]
    public void SuccessOutcome_ParsePacket_Throws_WhenIdIsEmpty()
    {
        var outcome = new ReceiveOutcome.SuccessOutcome(MakePacket(":path"));

        Assert.Throws<InvalidDataException>(() => outcome.ParsePacket());
    }

    [Fact]
    public void SuccessOutcome_ParsePacket_Throws_WhenDataMissing()
    {
        var outcome = new ReceiveOutcome.SuccessOutcome(new JsonObject { ["id"] = "test:c2s" });

        Assert.Throws<InvalidDataException>(() => outcome.ParsePacket());
    }

    [Fact]
    public void SuccessOutcome_ParsePacket_Throws_WhenDataIsNotObject()
    {
        var outcome = new ReceiveOutcome.SuccessOutcome(new JsonObject
        {
            ["id"] = "test:c2s",
            ["data"] = "not-an-object"
        });

        Assert.Throws<InvalidDataException>(() => outcome.ParsePacket());
    }

    [Fact]
    public void SuccessOutcome_TryParsePacket_ValidatesIdFormat()
    {
        var outcome = new ReceiveOutcome.SuccessOutcome(MakePacket("bad"));

        Assert.False(outcome.TryParsePacket(out Identifier? id, out JsonObject? data));
        Assert.Null(id);
        Assert.Null(data);
    }

    [Fact]
    public void ClosedOutcome_IsFailure_AndIfFailedPassesNullException()
    {
        var outcome = new ReceiveOutcome.ClosedOutcome(
            "closed",
            WebSocketCloseStatus.NormalClosure,
            "bye");

        Assert.False(outcome.IsSuccess);
        Assert.NotNull(outcome.AsClosedOutcome());
        Assert.Null(outcome.AsSuccessOutcome());
        Assert.Null(outcome.AsAbortedOutcome());
        Assert.Null(outcome.AsInvalidOutcome());
        Assert.Equal(WebSocketCloseStatus.NormalClosure, outcome.AsClosedOutcome()!.CloseStatus);
        Assert.Equal("bye", outcome.AsClosedOutcome()!.CloseStatusDescription);
        Assert.Throws<InvalidOperationException>(() => outcome.GetOrThrow());

        string? message = null;
        Exception? exception = new Exception("should be null");
        outcome.IfFailed((m, e) =>
        {
            message = m;
            exception = e;
        });

        Assert.Equal("closed", message);
        Assert.Null(exception);
    }

    [Fact]
    public void AbortedOutcome_IsFailure_AndIfFailedPassesException()
    {
        var original = new IOException("connection reset");
        var outcome = new ReceiveOutcome.AbortedOutcome(original);

        Assert.False(outcome.IsSuccess);
        Assert.NotNull(outcome.AsAbortedOutcome());
        Assert.Same(original, outcome.AsAbortedOutcome()!.Exception);
        Assert.Equal(original.Message, outcome.AsAbortedOutcome()!.Message);

        string? message = null;
        Exception? exception = null;
        outcome.IfFailed((m, e) =>
        {
            message = m;
            exception = e;
        });

        Assert.Equal(original.Message, message);
        Assert.Same(original, exception);

        var thrown = Assert.Throws<IOException>(() => outcome.GetOrThrow());
        Assert.Same(original, thrown);
    }

    [Fact]
    public void InvalidOutcome_IsFailure_AndIfFailedPassesException()
    {
        var original = new InvalidDataException("bad json");
        var outcome = new ReceiveOutcome.InvalidOutcome("bad json", original);

        Assert.False(outcome.IsSuccess);
        Assert.NotNull(outcome.AsInvalidOutcome());
        Assert.Same(original, outcome.AsInvalidOutcome()!.Exception);
        Assert.Equal("bad json", outcome.AsInvalidOutcome()!.Message);

        string? message = null;
        Exception? exception = null;
        outcome.IfFailed((m, e) =>
        {
            message = m;
            exception = e;
        });

        Assert.Equal("bad json", message);
        Assert.Same(original, exception);

        var thrown = Assert.Throws<InvalidDataException>(() => outcome.GetOrThrow());
        Assert.Same(original, thrown);
    }

    [Fact]
    public void TryParsePacket_OnFailure_ReturnsFalseWithNulls()
    {
        var outcome = new ReceiveOutcome.InvalidOutcome("bad json", new InvalidDataException("bad json"));

        Assert.False(outcome.TryParsePacket(out Identifier? id, out JsonObject? data));
        Assert.Null(id);
        Assert.Null(data);
    }

    [Fact]
    public void IfSuccess_And_IfFailed_AreChainable_AndOnlyInvokeMatchingBranch()
    {
        var success = new ReceiveOutcome.SuccessOutcome(MakePacket());
        int successCalls = 0;
        int failedCalls = 0;

        ReceiveOutcome returned = success
            .IfSuccess(_ => { successCalls++; })
            .IfFailed((_, _) => { failedCalls++; });

        Assert.Same(success, returned);
        Assert.Equal(1, successCalls);
        Assert.Equal(0, failedCalls);

        var closed = new ReceiveOutcome.ClosedOutcome("closed");
        successCalls = 0;
        failedCalls = 0;

        returned = closed
            .IfSuccess(_ => { successCalls++; })
            .IfFailed((_, _) => { failedCalls++; });

        Assert.Same(closed, returned);
        Assert.Equal(0, successCalls);
        Assert.Equal(1, failedCalls);
    }
}
