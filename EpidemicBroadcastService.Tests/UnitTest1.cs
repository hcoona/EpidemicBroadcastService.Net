using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Clocks;
using EpidemicBroadcastService.Extensions;
using Grpc.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Xunit;
using Microsoft.Extensions.Options;
using Xunit;
using Xunit.Abstractions;
using Any = Google.Protobuf.WellKnownTypes.Any;

namespace EpidemicBroadcastService.Tests
{
    public class UnitTest1
    {
        private readonly ITestOutputHelper testOutputHelper;
        private readonly LoggerFactory loggerFactory;

        public UnitTest1(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
            this.loggerFactory = new LoggerFactory(new[] { new XunitLoggerProvider(testOutputHelper) });
        }

        [Fact]
        public void Test1()
        {
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    { "RoundIntervalMilliseconds", "200" }
                })
                .Build();

            var services = new ServiceCollection();
            services.AddSingleton(configuration);
            services.AddOptions();
            services.Configure<EpidemicBroadcastServiceOption>(configuration);

            var container = services.BuildServiceProvider();

            var option = container.GetRequiredService<IOptions<EpidemicBroadcastServiceOption>>().Value;
            Assert.Equal(200.0, option.RoundIntervalMilliseconds);
        }

        [Fact]
        public void TestAny()
        {
            var payload = Any.Pack(new PushResponse());
            Assert.Equal("EpidemicBroadcastService.PushResponse", payload.TypeUrl.Split('/').Last());
        }

        [Fact]
        public void TestRandomTake()
        {
            var random = new Random();
            var source = Enumerable.Range(0, 20);

            this.testOutputHelper.WriteLine(string.Join(", ", EnumerableExtensions.RandomTake(source, random, 5)));

            Assert.Equal(5, EnumerableExtensions.RandomTake(source, random, 5).Count());
            Assert.Equal(20, EnumerableExtensions.RandomTake(source, random, 200).Count());
            Assert.Empty(EnumerableExtensions.RandomTake(Enumerable.Empty<int>(), random, 5));
        }

        [Fact]
        public void TestAccessNotExistDictionaryKey()
        {
            var dict = new Dictionary<string, int>();
            dict["test"] = dict.GetValueOrDefault("test") + 1;

            Assert.Equal(1, dict["test"]);
        }

        [Fact]
        public async Task TestGrpc()
        {
            const int BINDING_PORT = 28888;

            var logger = loggerFactory.CreateLogger(nameof(TestGrpc));

            var gossiperImpl = new GossiperImpl();
            gossiperImpl.OnPull += (_, e) => { logger.LogInformation("OnPull"); e.Rumors = Enumerable.Empty<Rumor>(); };
            gossiperImpl.OnPush += (_, e) => logger.LogInformation("Receive rumors: [{0}]", string.Join(", ", e.Rumors.Select(r => r.GetHashCode())));

            var server = new Server
            {
                Ports = { new ServerPort("127.0.0.1", BINDING_PORT, ServerCredentials.Insecure) },
                Services = { Gossiper.BindService(gossiperImpl) }
            };
            server.Start();

            var client = new Gossiper.GossiperClient(new Channel($"127.0.0.1:{BINDING_PORT}", ChannelCredentials.Insecure));
            var pullResult = client.Pull(new PullRequest(), deadline: DateTime.UtcNow.AddMilliseconds(200));
            while (await pullResult.ResponseStream.MoveNext())
            {
                logger.LogInformation("Pulled rumor {0}", pullResult.ResponseStream.Current.GetHashCode());
            }

            var pushCall = client.Push(deadline: DateTime.UtcNow.AddMilliseconds(200));
            await pushCall.RequestStream.WriteAsync(new Rumor
            {
                Counter = 1,
                Payload = new Any()
            });
            await pushCall.RequestStream.CompleteAsync();
            var pushResult = await pushCall;
            logger.LogInformation("Client push finished");

            await server.ShutdownAsync();
        }

        [Fact]
        public async Task TestObserveService()
        {
            const int BINDING_PORT = 38888;

            var logger = loggerFactory.CreateLogger(nameof(TestObserveService));

            var options = Options.Create(new EpidemicBroadcastServiceOption
            {
                MinimalSleepIntervalMilliseconds = 10,
                RoundIntervalMilliseconds = 1000,
                EstimatedNodeCountMin = 5,
                FanInNodeCountMax = 1,
                FanOutNodeCountMax = 2,
                PushRoundsFactor = 1,
                PullRoundsFactor = 1,
                DeadRoundsFactor = 1
            });
            var stopwatchProvider = new SystemStopwatchProvider();

            var service = new EpidemicBroadcastService(
                loggerFactory.CreateLogger<EpidemicBroadcastService>(),
                options,
                stopwatchProvider)
            {
                GossiperClientFactory = target =>
                    new Gossiper.GossiperClient(new Channel(target, ChannelCredentials.Insecure)),
                MemberListProvider = () => Enumerable.Range(BINDING_PORT, 5).Select(p => $"127.0.0.1:{p}").ToArray()
            };

            var server = new Server
            {
                Ports = { new ServerPort("127.0.0.1", BINDING_PORT, ServerCredentials.Insecure) },
                Services = { Gossiper.BindService(service.GossiperImpl) }
            };

            service.OnRumorReceived += (_, e) => logger.LogInformation("Rumor {0} received.", e.Payload.GetHashCode());

            server.Start();
            await service.StartAsync();

            var client = new Gossiper.GossiperClient(new Channel($"127.0.0.1:{BINDING_PORT}", ChannelCredentials.Insecure));
            var pullResult = client.Pull(new PullRequest(), deadline: DateTime.UtcNow.AddMilliseconds(200));
            while (await pullResult.ResponseStream.MoveNext())
            {
                logger.LogInformation("Pulled rumor {0}", pullResult.ResponseStream.Current.GetHashCode());
            }

            var pushCall = client.Push(deadline: DateTime.UtcNow.AddMilliseconds(200));
            await pushCall.RequestStream.WriteAsync(new Rumor
            {
                Counter = 1,
                Payload = new Any()
            });
            await pushCall.RequestStream.CompleteAsync();
            var pushResult = await pushCall;
            logger.LogInformation("Client push finished");

            await service.StopAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task TestObservePullService()
        {
            const int BINDING_PORT = 38888;

            var logger = loggerFactory.CreateLogger(nameof(TestObserveService));

            var options = Options.Create(new EpidemicBroadcastServiceOption
            {
                MinimalSleepIntervalMilliseconds = 10,
                RoundIntervalMilliseconds = 100,
                EstimatedNodeCountMin = 5,
                FanInNodeCountMax = 1,
                FanOutNodeCountMax = 2,
                PushRoundsFactor = 1,
                PullRoundsFactor = 1,
                DeadRoundsFactor = 1
            });
            var stopwatchProvider = new SystemStopwatchProvider();

            var service = new EpidemicBroadcastService(
                loggerFactory.CreateLogger<EpidemicBroadcastService>(),
                options,
                stopwatchProvider)
            {
                GossiperClientFactory = target =>
                    new Gossiper.GossiperClient(new Channel(target, ChannelCredentials.Insecure)),
                MemberListProvider = () => Enumerable.Range(BINDING_PORT, 5).Select(p => $"127.0.0.1:{p}").ToArray()
            };

            var server = new Server
            {
                Ports = { new ServerPort("127.0.0.1", BINDING_PORT, ServerCredentials.Insecure) },
                Services = { Gossiper.BindService(service.GossiperImpl) }
            };

            service.OnRumorReceived += (_, e) => logger.LogInformation("Rumor {0} received.", e.Payload.GetHashCode());

            server.Start();
            await service.StartAsync();

            service.Broadcast(new Any());
            await Task.Delay(500);

            var client = new Gossiper.GossiperClient(new Channel($"127.0.0.1:{BINDING_PORT}", ChannelCredentials.Insecure));

            using (var pushCall = client.Push(deadline: DateTime.UtcNow.AddMilliseconds(200)))
            {
                await pushCall.RequestStream.WriteAsync(new Rumor
                {
                    Counter = 3,
                    Payload = new Any()
                });
                await pushCall.RequestStream.CompleteAsync();
                var pushResult = await pushCall;
                logger.LogInformation("Client push finished");
            }

            await Task.Delay(110);
            using (var pullResult = client.Pull(new PullRequest(), deadline: DateTime.UtcNow.AddMilliseconds(200)))
            {
                while (await pullResult.ResponseStream.MoveNext())
                {
                    logger.LogInformation("Pulled rumor {0}", pullResult.ResponseStream.Current.GetHashCode());
                }
                logger.LogInformation("Client pull finished");
            }

            await service.StopAsync();
            await server.ShutdownAsync();

            await Task.Delay(1000);
        }

        [Fact]
        public async Task TestIntegration()
        {
            const int NODE_COUNT = 20;
            const int STARTING_PORT = 18888;

            var logger = loggerFactory.CreateLogger(nameof(TestIntegration));
            logger.LogInformation("{0}({1}, {2})", nameof(TestIntegration), NODE_COUNT, STARTING_PORT);

            var options = Options.Create(new EpidemicBroadcastServiceOption
            {
                MinimalSleepIntervalMilliseconds = 10,
                RoundIntervalMilliseconds = 1000,
                EstimatedNodeCountMin = NODE_COUNT,
                FanInNodeCountMax = 1,
                FanOutNodeCountMax = 2,
                PushRoundsFactor = 1,
                PullRoundsFactor = 1,
                DeadRoundsFactor = 1
            });
            var stopwatchProvider = new SystemStopwatchProvider();

            var services = Enumerable.Repeat<object>(null, NODE_COUNT)
                .Select((_, idx) => new EpidemicBroadcastService(
                    (idx == 0 ? (ILoggerFactory)loggerFactory : NullLoggerFactory.Instance).CreateLogger<EpidemicBroadcastService>(),
                    options,
                    stopwatchProvider)
                {
                    GossiperClientFactory = target =>
                        new Gossiper.GossiperClient(new Channel(target, ChannelCredentials.Insecure)),
                    MemberListProvider = () => Enumerable.Range(STARTING_PORT, NODE_COUNT).Select(p => $"127.0.0.1:{p}").ToArray()
                })
                .ToArray();

            var servers = services.Select((s, i) => new Server
            {
                Ports = { new ServerPort("127.0.0.1", STARTING_PORT + i, ServerCredentials.Insecure) },
                Services = { Gossiper.BindService(s.GossiperImpl) }
            }).ToArray();

            foreach (var s in servers) s.Start();
            services.Select<EpidemicBroadcastService, object>((s, idx) =>
            {
                s.OnRumorReceived += (_, e) => logger.LogWarning(
                    "Rumor {0} received at service {1}!",
                    e.Payload.GetHashCode(),
                    idx);
                return null;
            });

            await Task.WhenAll(services.Select(s => s.StartAsync()));
            services[0].Broadcast(new Any());

            await Task.Delay(1000 * 10);

            await Task.WhenAll(services.Select(s => s.StopAsync()));
            await Task.WhenAll(servers.Select(s => s.ShutdownAsync()));
        }
    }
}
