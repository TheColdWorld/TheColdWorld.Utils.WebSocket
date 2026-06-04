using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.VisualStudio.TestPlatform.TestHost;
using Newtonsoft.Json.Linq;
using System.ComponentModel;
using System.Text.Json.Nodes;
using TheColdWorld.Utils.socket;
using TheColdWorld.Utils.WebSocket;
using static TheColdWorld.Utils.WebSocket.WebSocketServerControlUnit;
using WebSocketClient = TheColdWorld.Utils.WebSocket.WebSocketClient;

namespace TheColdWorld.Utils.Test;

class TestPacket : IPacket
{
    public TestPacket(PacketBindSide packetBindSide, Identifier identifier, JsonObject data)
    {
        PacketBindSide = packetBindSide;
        Identifier = identifier;
        Data = data;
    }
    public TestPacket(JsonObject packet)
    {
        PacketBindSide = Enum.Parse<PacketBindSide>(packet[nameof(PacketBindSide)].AsValue().ToString());
        Identifier = new(packet[nameof(Identifier)].AsValue().ToString());
        Data = packet[nameof(Data)].AsObject();
    }
    public PacketBindSide PacketBindSide { get; }

    public Identifier Identifier { get; }
    public JsonObject Data { get; }

    public JsonObject Write() => new() { [nameof(PacketBindSide)] = PacketBindSide.ToString(), [nameof(Identifier)] = Identifier.ToString(), [nameof(Data)]=Data.DeepClone() };
}
public class HelperTest
{
    [Theory]
    [InlineData("test:c2s", "fuck you NVIDIA,你好")]
    [InlineData("test:s2c", "11451419191080")]
    public void TestPacketParse(string id,string content)
    {
        TestPacket packet = new(PacketBindSide.ServerBind, new(id), new() { [nameof(content)]=content});
        Assert.Equal(id, packet.Identifier);
        var encoded= WebsocketHelper.BuildPacket(packet);
        var node= JsonNode.Parse(encoded);
        Assert.IsType<JsonObject>(node);
        var obj = node.AsObject();
        Assert.True(WebsocketHelper.TryParsePacket(obj, out var eid, out var eData));
        Assert.Equal(id,eid);
        Assert.Equal(packet.Write().ToJsonString(),eData.ToJsonString());
    }
}

public class OnlineTest : IAsyncLifetime
{
    private IHost _host;
    private int _port;
    private string _serverUri;
    private CancellationTokenSource cts = new();
    private WebSocketServerControlUnit controlUnit = new();
    public async Task DisposeAsync()
    {
        await cts.CancelAsync();
        await _host.StopAsync();
        cts.Dispose();
        _host.Dispose();
    }

    public async Task InitializeAsync()
    {
        _host = new HostBuilder().ConfigureWebHost(builder =>
        {
            builder.UseKestrel();
            builder.UseUrls("http://127.0.0.1:0");
            builder.Configure(app =>
            {
                app.UseWebSockets();
                app.Use(async (context, next) =>
                {
                    if (context.Request.Path.Value is not null && context.Request.Path.Value=="/ws")
                    {
                        if(context.WebSockets.IsWebSocketRequest)
                        {
                            var client= await context.WebSockets.AcceptWebSocketAsync();
                            await controlUnit.HandleClient(client);
                        }
                        else context.Response.StatusCode = StatusCodes.Status400BadRequest;
                        return;
                    }
                    await next();
                });
            });
        }).Build();
        await _host.StartAsync();
        var addresses = _host.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        _port = new Uri(addresses.Addresses.First()).Port;
        _serverUri = $"ws://127.0.0.1:{_port}/ws";
    }
    [Theory]
    [InlineData("fuck you NVIDIA,你好")]
    [InlineData("11451419191080")]
    public async Task TestC2SMessage(string message)
    {
        WebSocketClient client = new(new(_serverUri));
        await client.ConnectAsync();
        var tcs = new TaskCompletionSource<JsonObject>();
        async Task HandlePacketAsync(Identifier id, JsonObject dataObj, SendToClientAsync sendToClient, Action stopAction, CancellationToken token)
        {
            tcs.SetResult(dataObj);
        }
        TestPacket rightPacket = new(PacketBindSide.ServerBind, new("test:c2s"), new() { [nameof(message)]=message });
        TestPacket wrongPacket = new(PacketBindSide.ClientBind, new("failed:failed"), []);
        controlUnit.OnPacketAccepted += HandlePacketAsync;
        await Assert.ThrowsAsync<ArgumentException>(async () => { await client.SendAsync(wrongPacket); });
        await client.SendAsync(rightPacket);
        await Task.Delay(1000);
        Assert.Equal(rightPacket.Write().ToJsonString(), (await tcs.Task).ToJsonString());
        TestPacket p = new(await tcs.Task);
        Assert.Equal(message, ((string?)p.Data[nameof(message)]?.AsValue()));
        controlUnit.OnPacketAccepted -= HandlePacketAsync;
    }
    [Theory]
    [InlineData("fuck you NVIDIA,你好")]
    [InlineData("11451419191080")]
    public async Task TestS2CMessage(string message)
    {
        WebSocketClient client = new(new(_serverUri));
        await client.ConnectAsync();
        TestPacket rightPacket = new(PacketBindSide.ClientBind, new("test:s2c"), new() { [nameof(message)] = message });
        TestPacket wrongPacket = new(PacketBindSide.ServerBind, new("failed:failed"), []);
        await Assert.ThrowsAsync<ArgumentException>(async () => { await controlUnit.BroadCastAsync(wrongPacket); });
        await controlUnit.BroadCastAsync(rightPacket);
        var packet= await client.ReceiveAsync();
        Assert.Equal(rightPacket.Identifier.ToString(),packet.Item1.ToString());
        Assert.Equal(rightPacket.Write().ToJsonString(), packet.Item2.ToJsonString());
        TestPacket r = new(packet.Item2);
        Assert.Equal(message, ((string?)r.Data[nameof(message)]?.AsValue()));
    }
}
