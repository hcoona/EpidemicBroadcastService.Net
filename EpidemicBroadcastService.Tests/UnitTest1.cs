using System;
using System.Collections.Generic;
using System.Linq;
using EpidemicBroadcastService.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;
using Xunit.Abstractions;

namespace EpidemicBroadcastService.Tests
{
    public class UnitTest1
    {
        private readonly ITestOutputHelper testOutputHelper;

        public UnitTest1(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
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
            var payload = Google.Protobuf.WellKnownTypes.Any.Pack(new PushResponse());
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
    }
}
