using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Clocks;
using EpidemicBroadcastService.Extensions;
using Microsoft.Extensions.Logging;
using Any = Google.Protobuf.WellKnownTypes.Any;

namespace EpidemicBroadcastService
{
    internal sealed class EpidemicBroadcastServiceImpl : IDisposable
    {
        private readonly ILogger logger;
        private readonly EpidemicBroadcastServiceOption options;
        private readonly IStopwatchProvider stopwatchProvider;
        private readonly GossiperImpl gossiperImpl;
        private readonly Func<IReadOnlyList<string>> memberListProvider;
        private readonly Func<string, Gossiper.GossiperClient> gossiperClientFactory;
        private readonly EventHandler<RumorReceivedEventArgs> onRumorReceived;

        private readonly CancellationTokenSource backgroundTaskCancellationTokenSource;
        private readonly Task backgroundTask;

        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1);
        private readonly IDictionary<Any, int> pushStateRumorCounterDictionary = new Dictionary<Any, int>();
        private readonly IDictionary<Any, int> pullStateRumorCounterDictionary = new Dictionary<Any, int>();
        private readonly IDictionary<Any, int> deadStateRumorCounterDictionary = new Dictionary<Any, int>();
        private readonly IDictionary<Any, int> rumorLowerCounterDictionary = new Dictionary<Any, int>();
        private readonly IDictionary<Any, int> rumorHigherCounterDictionary = new Dictionary<Any, int>();

        public EpidemicBroadcastServiceImpl(
            ILogger logger,
            EpidemicBroadcastServiceOption options,
            IStopwatchProvider stopwatchProvider,
            GossiperImpl gossiperImpl,
            Func<IReadOnlyList<string>> memberListProvider,
            Func<string, Gossiper.GossiperClient> gossiperClientFactory,
            EventHandler<RumorReceivedEventArgs> onRumorReceived)
        {
            this.logger = logger;
            this.options = options;
            this.stopwatchProvider = stopwatchProvider;
            this.gossiperImpl = gossiperImpl;
            this.memberListProvider = memberListProvider;
            this.gossiperClientFactory = gossiperClientFactory;
            this.onRumorReceived = onRumorReceived;

            gossiperImpl.OnPull += GossiperImpl_OnPull;
            gossiperImpl.OnPush += GossiperImpl_OnPush;

            backgroundTaskCancellationTokenSource = new CancellationTokenSource();
            backgroundTask = Task.Factory.StartNew(
                () => RunBackgroundTaskAsync(backgroundTaskCancellationTokenSource.Token).GetAwaiter().GetResult(),
                backgroundTaskCancellationTokenSource.Token,
                TaskCreationOptions.LongRunning,
                Task.Factory.Scheduler);
        }

        public void Dispose()
        {
            gossiperImpl.OnPull -= GossiperImpl_OnPull;
            gossiperImpl.OnPush -= GossiperImpl_OnPush;

            backgroundTaskCancellationTokenSource.Cancel();
            backgroundTask.GetAwaiter().GetResult();
        }

        private async Task RunBackgroundTaskAsync(CancellationToken cancellationToken)
        {
            var sleepMinimalInterval = TimeSpan.FromMilliseconds(options.MinimalSleepIntervalMilliseconds);
            var roundInterval = TimeSpan.FromMilliseconds(options.RoundIntervalMilliseconds);

            var random = new Random();
            var stopwatch = stopwatchProvider.Create();
            while (!cancellationToken.IsCancellationRequested)
            {
                stopwatch.Restart();

                /* Background jobs
                 *
                 * 0. Update member list & related counter threshold
                 * 1. Calculate the counters in previous round
                 * 2. Send push requests
                 * 3. Send pull requests (switch to C states & clear lower, higher counter)
                 */
                try
                {
                    IReadOnlyList<Rumor> rumorsToBeSent = new Rumor[0];
                    var memberList = memberListProvider.Invoke();
                    await semaphore.ProtectAsync(() =>
                    {
                        UpdateCounterThresholds(memberList);
                        UpdateRumorCountersFromPreviousRound();

                        rumorsToBeSent = pushStateRumorCounterDictionary
                            .Select(p => new Rumor
                            {
                                Counter = p.Value,
                                Payload = p.Key
                            })
                            .ToArray();
                    }, cancellationToken);

                    // 2. Send push requests
                    var activePushTask = ActivePushRumors(random, memberList, rumorsToBeSent, cancellationToken);

                    // 3. Send pull requests (switch to C states & clear lower, higher counter)
                    var pulledRumors = await ActivePullRumors(random, memberList, cancellationToken);
                    var activePullTask = semaphore.ProtectAsync(() =>
                    {
                        foreach (var rumor in pulledRumors)
                        {
                            ReceivePulledRumor(rumor);
                        }
                    });

                    await Task.WhenAll(activePushTask, activePullTask);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.ToString());
                }

                stopwatch.Stop();
                var sleepInterval = roundInterval - stopwatch.Elapsed;
                if (sleepInterval > sleepMinimalInterval)
                {
                    await Task.Delay(sleepInterval);
                }
                else
                {
                    logger.LogWarning("The execution time too long: {0}", stopwatch.Elapsed);
                }
            }
        }

        private int pushCounterThreshold;
        private int pullCounterThreshold;
        private int deadCounterThreshold;

        private void UpdateCounterThresholds(IReadOnlyList<string> memberList)
        {
            var memberCount = Math.Max(options.EstimatedNodeCountMin, memberList.Count);
            var doubleLogMemberCount = Math.Log(Math.Log(memberCount));
            pushCounterThreshold = (int)(options.PushRoundsFactor * doubleLogMemberCount);
            pullCounterThreshold = (int)(options.PullRoundsFactor * doubleLogMemberCount);
            deadCounterThreshold = (int)(options.DeadRoundsFactor * doubleLogMemberCount);
        }

        private void UpdateRumorCountersFromPreviousRound()
        {
            var rumorsNeedGrantFromPreviousRound = rumorLowerCounterDictionary.Keys
                .Union(rumorHigherCounterDictionary.Keys);
            foreach (var rumor in rumorsNeedGrantFromPreviousRound)
            {
                if (rumorLowerCounterDictionary[rumor] < rumorHigherCounterDictionary[rumor])
                {
                    if (pushStateRumorCounterDictionary.ContainsKey(rumor))
                    {
                        if (pushStateRumorCounterDictionary.IncreaseCounter(rumor) > pushCounterThreshold)
                        {
                            logger.LogDebug(
                                "Grant rumor {0} from push state to pull state because median counter rule.",
                                rumor.GetHashCode());
                            pullStateRumorCounterDictionary[rumor] = 0;
                            pushStateRumorCounterDictionary.Remove(rumor);
                        }
                    }
                }
            }

            foreach (var rumor in pullStateRumorCounterDictionary.Keys)
            {
                if (pullStateRumorCounterDictionary[rumor]++ > pullCounterThreshold)
                {
                    logger.LogDebug(
                        "Grant rumor {0} from pull state to dead state because reach threshold.",
                        rumor.GetHashCode());
                    deadStateRumorCounterDictionary.Add(rumor, 0);
                    pullStateRumorCounterDictionary.Remove(rumor);
                }
            }

            foreach (var rumor in deadStateRumorCounterDictionary.Keys)
            {
                if (deadStateRumorCounterDictionary[rumor]++ > deadCounterThreshold)
                {
                    logger.LogDebug(
                        "Evict rumor {0} from dead state because reach threshold.",
                        rumor.GetHashCode());
                    deadStateRumorCounterDictionary.Remove(rumor);
                }
            }

            rumorLowerCounterDictionary.Clear();
            rumorHigherCounterDictionary.Clear();
        }

        private async Task ActivePushRumors(
            Random random,
            IReadOnlyList<string> memberList,
            IReadOnlyList<Rumor> rumorsToBeSent,
            CancellationToken cancellationToken)
        {
            var fanOutNodeCount = 1 + random.Next(options.FanOutNodeCountMax);
            var fanOutClients = memberList
                .RandomTake(random, fanOutNodeCount)
                .Select(addr => gossiperClientFactory.Invoke(addr));

            var pushTasks = fanOutClients.AsParallel()
                .Select(async c =>
                {
                    var pushTask = c.Push();
                    foreach (var rumor in rumorsToBeSent)
                    {
                        if (cancellationToken.IsCancellationRequested) break;
                        await pushTask.RequestStream.WriteAsync(rumor);
                    }
                    await pushTask;
                });
            await Task.WhenAll(pushTasks);
        }

        private async Task<IEnumerable<Rumor>> ActivePullRumors(
            Random random,
            IReadOnlyList<string> memberList,
            CancellationToken cancellationToken)
        {
            var fanInNodeCount = 1 + random.Next(options.FanInNodeCountMax);
            var fanInClients = memberList
                .RandomTake(random, fanInNodeCount)
                .Select(addr => gossiperClientFactory.Invoke(addr));

            var pullTasks = fanInClients.AsParallel()
                .Select(c => ActivePullRumors(c, cancellationToken));
            await Task.WhenAll(pullTasks);

            return pullTasks.SelectMany(t => t.GetAwaiter().GetResult());
        }

        private async Task<IList<Rumor>> ActivePullRumors(
            Gossiper.GossiperClient client,
            CancellationToken cancellationToken)
        {
            var results = new List<Rumor>();
            var pullTask = client.Pull(new PullRequest());
            while (!cancellationToken.IsCancellationRequested
                && await pullTask.ResponseStream.MoveNext(cancellationToken))
            {
                results.Add(pullTask.ResponseStream.Current);
            }
            return results;
        }

        private void GossiperImpl_OnPush(object sender, PushRumorEventArgs e)
        {
            Task.Run(() => semaphore.ProtectAsync(() =>
            {
                foreach (var rumor in e.Rumors)
                {
                    ReceivePushedRumor(rumor);
                }
            }));
        }

        private void GossiperImpl_OnPull(object sender, PullRumorEventArgs e)
        {
            semaphore.Protect(() =>
            {
                e.Rumors = pullStateRumorCounterDictionary
                    .Select(p => new Rumor
                    {
                        Counter = p.Value,
                        Payload = p.Key
                    });
            });
        }

        private void ReceivePushedRumor(Rumor rumor)
        {
            var payload = rumor.Payload;
            if (pushStateRumorCounterDictionary.ContainsKey(payload))
            {
                if (rumor.Counter < pushStateRumorCounterDictionary[payload])
                {
                    rumorLowerCounterDictionary.IncreaseCounter(payload);
                }
                else
                {
                    rumorHigherCounterDictionary.IncreaseCounter(payload);
                }
            }
            else
            {
                if (!pullStateRumorCounterDictionary.ContainsKey(payload)
                    && !deadStateRumorCounterDictionary.ContainsKey(payload))
                {
                    logger.LogDebug("New rumor {0} received in push request", payload.GetHashCode());
                    onRumorReceived?.Invoke(this, new RumorReceivedEventArgs
                    {
                        Payload = payload
                    });
                    pushStateRumorCounterDictionary.Add(payload, 1);
                }
            }
        }

        private void ReceivePulledRumor(Rumor rumor)
        {
            var payload = rumor.Payload;
            if (pushStateRumorCounterDictionary.ContainsKey(payload))
            {
                pushStateRumorCounterDictionary.Remove(payload);
                pullStateRumorCounterDictionary.Add(payload, 1);
                logger.LogDebug(
                    "Grant rumor {0} from push state to pull state because received in pull response.",
                    payload.GetHashCode());
            }
            else if (!pullStateRumorCounterDictionary.ContainsKey(payload)
                && !deadStateRumorCounterDictionary.ContainsKey(payload))
            {
                logger.LogDebug("New rumor {0} received in pull response", payload.GetHashCode());
                onRumorReceived?.Invoke(this, new RumorReceivedEventArgs
                {
                    Payload = payload
                });
                pullStateRumorCounterDictionary.Add(payload, 1);
            }
        }
    }
}
