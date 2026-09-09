using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Net.WebSockets;
using System.Text;
using TheColdWorld.Utils.WebSocket;

namespace TheColdWorld.Utils.Test;

public sealed class WebSocketTestHost : IAsyncDisposable
{
    private readonly IHost _host;
    private readonly CancellationTokenSource _cts = new();

    public WebSocketServerControlUnit ControlUnit { get; } = new();

    public Uri ServerUri { get; private set; } = null!;
    public Uri RawUri { get; private set; } = null!;
    public Uri InvalidUri { get; private set; } = null!;


    public TaskCompletionSource<System.Net.WebSockets.WebSocket> ServerSocket { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);


    public TaskCompletionSource<Exception?> HandleClientCompletion { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);


    public TaskCompletionSource<System.Net.WebSockets.WebSocket> RawServerSocket { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);


    public TaskCompletionSource RawDone { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public CancellationToken ServerToken => _cts.Token;

    public WebSocketTestHost()
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
                    string? path = context.Request.Path.Value;

                    if (path == "/ws" && context.WebSockets.IsWebSocketRequest)
                    {
                        System.Net.WebSockets.WebSocket client = await context.WebSockets.AcceptWebSocketAsync();
                        ServerSocket.TrySetResult(client);

                        Exception? error = null;
                        try
                        {
                            await ControlUnit.HandleClient(client, _cts.Token);
                        }
                        catch (Exception e)
                        {
                            error = e;
                        }
                        finally
                        {
                            HandleClientCompletion.TrySetResult(error);
                        }
                        return;
                    }

                    if (path == "/ws-raw" && context.WebSockets.IsWebSocketRequest)
                    {
                        System.Net.WebSockets.WebSocket client = await context.WebSockets.AcceptWebSocketAsync();
                        RawServerSocket.TrySetResult(client);
                        await RawDone.Task;
                        return;
                    }

                    if (path == "/ws-invalid" && context.WebSockets.IsWebSocketRequest)
                    {
                        System.Net.WebSockets.WebSocket client = await context.WebSockets.AcceptWebSocketAsync();
                        byte[] bad = Encoding.UTF8.GetBytes("{ this is not json");
                        await client.SendAsync(bad, WebSocketMessageType.Binary, true, _cts.Token);
                        try
                        {
                            await Task.Delay(Timeout.Infinite, _cts.Token);
                        }
                        catch (OperationCanceledException) { }
                        return;
                    }

                    await next();
                });
            });
        }).Build();
    }

    public async Task StartAsync()
    {
        await _host.StartAsync();
        IServerAddressesFeature addresses = _host.Services
            .GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()
            ?? throw new InvalidOperationException("IServerAddressesFeature is unavailable.");

        var baseUri = new Uri(addresses.Addresses.First());
        ServerUri = new Uri($"ws://{baseUri.Host}:{baseUri.Port}/ws");
        RawUri = new Uri($"ws://{baseUri.Host}:{baseUri.Port}/ws-raw");
        InvalidUri = new Uri($"ws://{baseUri.Host}:{baseUri.Port}/ws-invalid");
    }
    public void CancelServer() => _cts.Cancel();

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        await _host.StopAsync();
        _host.Dispose();
        _cts.Dispose();
    }
}
