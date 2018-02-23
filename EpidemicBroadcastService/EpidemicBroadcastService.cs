using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Clocks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Any = Google.Protobuf.WellKnownTypes.Any;

namespace EpidemicBroadcastService
{
    public sealed class EpidemicBroadcastService : IHostedService
    {
        public EpidemicBroadcastService(
            ILogger<EpidemicBroadcastService> logger,
            IOptions<EpidemicBroadcastServiceOption> options,
            IStopwatchProvider stopwatchProvider)
        {
            this.logger = logger;
            this.options = options.Value;
            this.stopwatchProvider = stopwatchProvider;

            gossiperImpl = new GossiperImpl();
        }

        private readonly ILogger<EpidemicBroadcastService> logger;
        private readonly EpidemicBroadcastServiceOption options;
        private readonly IStopwatchProvider stopwatchProvider;
        private readonly GossiperImpl gossiperImpl;

        public Gossiper.GossiperBase GossiperImpl => gossiperImpl;

        // TODO: Lock the value during service running.
        public Func<IReadOnlyList<string>> MemberListProvider { get; set; }
        public Func<string, Gossiper.GossiperClient> GossiperClientFactory { get; set; }
        public event EventHandler<RumorReceivedEventArgs> OnRumorReceived;

        private EpidemicBroadcastServiceImpl serviceImpl;

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (MemberListProvider == null)
            {
                throw new InvalidOperationException(nameof(MemberListProvider) + " cannot be null");
            }

            if (GossiperClientFactory == null)
            {
                throw new InvalidOperationException(nameof(GossiperClientFactory) + " cannot be null");
            }

            serviceImpl = new EpidemicBroadcastServiceImpl(
                logger,
                options,
                stopwatchProvider,
                gossiperImpl,
                MemberListProvider,
                GossiperClientFactory,
                OnRumorReceived);
            return Task.FromResult<object>(null);
        }

        public Task StopAsync(CancellationToken cancellationToken = default)
        {
            serviceImpl.Dispose();
            return Task.FromResult<object>(null);
        }

        public void Broadcast(Any rumor) => serviceImpl.Broadcast(rumor);
    }
}
