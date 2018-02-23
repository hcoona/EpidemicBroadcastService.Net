using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Clocks;
#if NETSTANDARD2_0
using Microsoft.Extensions.Hosting;
#endif

namespace EpidemicBroadcastService
{
#if NETSTANDARD2_0
    public sealed class EpidemicBroadcastService : IHostedService
#else
    public sealed class EpidemicBroadcastService
#endif
    {
        public EpidemicBroadcastService(
            IStopwatchProvider stopwatchProvider,
            EpidemicBroadcastServiceOption options)
        {
            this.stopwatchProvider = stopwatchProvider;
            this.options = options;

            gossiperImpl = new GossiperImpl();
        }

        private readonly IStopwatchProvider stopwatchProvider;
        private readonly EpidemicBroadcastServiceOption options;
        private readonly GossiperImpl gossiperImpl;

        public Gossiper.GossiperBase GossiperImpl => gossiperImpl;

        // TODO: Lock the value during service running.
        public Func<IReadOnlyList<string>> MemberListProvider { get; set; }
        public Func<string, Gossiper.GossiperClient> GossiperClientFactory { get; set; }
        public event EventHandler<RumorReceivedEventArgs> OnRumorReceived;

        private IDisposable serviceImpl;

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
                stopwatchProvider,
                options,
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
    }
}
