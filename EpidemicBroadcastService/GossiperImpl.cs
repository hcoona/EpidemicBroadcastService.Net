using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;

namespace EpidemicBroadcastService
{
    internal class GossiperImpl : Gossiper.GossiperBase
    {
        internal event EventHandler<PushRumorEventArgs> OnPush;

        internal event EventHandler<PullRumorEventArgs> OnPull;

        public override async Task<PushResponse> Push(
            IAsyncStreamReader<Rumor> requestStream,
            ServerCallContext context)
        {
            var rumors = new List<Rumor>();
            while (!context.CancellationToken.IsCancellationRequested
                && await requestStream.MoveNext(context.CancellationToken))
            {
                var rumor = requestStream.Current;
                rumors.Add(rumor);
            }

            OnPush?.Invoke(this, new PushRumorEventArgs
            {
                Rumors = rumors,
            });

            return new PushResponse();
        }

        public override async Task Pull(
            PullRequest request,
            IServerStreamWriter<Rumor> responseStream,
            ServerCallContext context)
        {
            var pullEventArgs = new PullRumorEventArgs();
            OnPull?.Invoke(this, pullEventArgs);

            if (pullEventArgs.Rumors != null)
            {
                using (var rumorsAsyncEnumerator = pullEventArgs.Rumors.GetEnumerator())
                {
                    while (!context.CancellationToken.IsCancellationRequested
                        && rumorsAsyncEnumerator.MoveNext())
                    {
                        await responseStream.WriteAsync(rumorsAsyncEnumerator.Current);
                    }
                }
            }
        }
    }
}
