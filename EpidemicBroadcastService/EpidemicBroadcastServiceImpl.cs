using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Clocks;
using EpidemicBroadcastService.Extensions;
using Microsoft.Extensions.Logging;
using Any = Google.Protobuf.WellKnownTypes.Any;
using GrpcException = Grpc.Core.RpcException;

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

            gossiperImpl.OnPull += PassivePullRumors;
            gossiperImpl.OnPush += PassivePushRumors;

            backgroundTaskCancellationTokenSource = new CancellationTokenSource();
            backgroundTask = Task.Factory.StartNew(
                () => RunBackgroundTaskAsync(backgroundTaskCancellationTokenSource.Token).GetAwaiter().GetResult(),
                backgroundTaskCancellationTokenSource.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Current);
        }

        public void Dispose()
        {
            gossiperImpl.OnPull -= PassivePullRumors;
            gossiperImpl.OnPush -= PassivePushRumors;

            backgroundTaskCancellationTokenSource.Cancel();
            backgroundTask.GetAwaiter().GetResult();
        }

        public void Broadcast(Any rumor)
        {
            onRumorReceived?.Invoke(this, new RumorReceivedEventArgs
            {
                Payload = rumor
            });
            pushStateRumorCounterDictionary.Add(rumor, 1);
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
                        logger.LogDebug("Counter thresholds updated");

                        UpdateRumorCountersFromPreviousRound();
                        logger.LogDebug("Rumor counters updated");

                        rumorsToBeSent = pushStateRumorCounterDictionary
                            .Select(p => new Rumor
                            {
                                Counter = p.Value,
                                Payload = p.Key
                            })
                            .ToArray();
                    }, cancellationToken);
                    
                    // 2. Send push requests
                    logger.LogDebug("Pushing {0} rumors", rumorsToBeSent.Count);
                    var activePushTask = ActivePushRumors(random, memberList, rumorsToBeSent, cancellationToken);

                    // 3. Send pull requests (switch to C states & clear lower, higher counter)
                    var pulledRumors = (await ActivePullRumors(random, memberList, cancellationToken)).ToArray();
                    logger.LogDebug("Pulled {0} rumors", pulledRumors.Count());
                    var activePullTask = semaphore.ProtectAsync(() =>
                    {
                        foreach (var rumor in pulledRumors)
                        {
                            ReceivePulledRumor(rumor);
                        }
                    }, cancellationToken);

                    await Task.WhenAll(activePushTask, activePullTask);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.Message);
                }

                stopwatch.Stop();
                logger.LogDebug("Background loop finished in {0}ms", stopwatch.Elapsed.TotalMilliseconds);
                var sleepInterval = roundInterval - stopwatch.Elapsed;
                if (sleepInterval > sleepMinimalInterval)
                {
                    logger.LogDebug("Background loop sleep {0}ms", sleepInterval.TotalMilliseconds);
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

            pushCounterThreshold = Math.Max(1, (int)(options.PushRoundsFactor * doubleLogMemberCount));
            logger.LogTrace("{0} = {1}", nameof(pushCounterThreshold), pushCounterThreshold);

            pullCounterThreshold = Math.Max(1, (int)(options.PullRoundsFactor * doubleLogMemberCount));
            logger.LogTrace("{0} = {1}", nameof(pullCounterThreshold), pullCounterThreshold);

            deadCounterThreshold = Math.Max(1, (int)(options.DeadRoundsFactor * doubleLogMemberCount));
            logger.LogTrace("{0} = {1}", nameof(deadCounterThreshold), deadCounterThreshold);
        }

        private void UpdateRumorCountersFromPreviousRound()
        {
            var rumorsNeedGrantFromPreviousRound = rumorLowerCounterDictionary.Keys
                .Union(rumorHigherCounterDictionary.Keys)
                .ToArray();
            foreach (var rumor in rumorsNeedGrantFromPreviousRound)
            {
                if (rumorLowerCounterDictionary.GetOrDefault(rumor) < rumorHigherCounterDictionary.GetOrDefault(rumor))
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

            foreach (var rumor in pullStateRumorCounterDictionary.Keys.ToArray())
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

            foreach (var rumor in deadStateRumorCounterDictionary.Keys.ToArray())
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
            var fanOutNodes = memberList.RandomTake(random, fanOutNodeCount).ToArray();
            logger.LogInformation("Pushing to nodes [{0}]", string.Join(",", fanOutNodes));

            var pushTasks = fanOutNodes.AsParallel()
                .Select(async target =>
                {
                    try
                    {
                        var client = gossiperClientFactory.Invoke(target);
                        var pushTask = client.Push(
                            deadline: DateTime.UtcNow.AddMilliseconds(options.RoundIntervalMilliseconds / 2),
                            cancellationToken: cancellationToken);
                        foreach (var rumor in rumorsToBeSent)
                        {
                            if (cancellationToken.IsCancellationRequested) break;
                            await pushTask.RequestStream.WriteAsync(rumor);
                        }
                        await pushTask.RequestStream.CompleteAsync();
                        await pushTask;
                    }
                    catch (GrpcException ex)
                    {
                        logger.LogError(ex, "Failed to push rumor to {0} because {1}", target, ex.Message);
                    }
                });
            await Task.WhenAll(pushTasks);
        }

        private async Task<IEnumerable<Rumor>> ActivePullRumors(
            Random random,
            IReadOnlyList<string> memberList,
            CancellationToken cancellationToken)
        {
            var fanInNodeCount = 1 + random.Next(options.FanInNodeCountMax);
            var fanInNodes = memberList.RandomTake(random, fanInNodeCount).ToArray();
            logger.LogInformation("Pulling from nodes [{0}]", string.Join(",", fanInNodes));

            var pullTasks = fanInNodes.AsParallel()
                .Select(target => ActivePullRumors(target, cancellationToken));
            await Task.WhenAll(pullTasks);

            return pullTasks.SelectMany(t => t.GetAwaiter().GetResult());
        }

        private async Task<IList<Rumor>> ActivePullRumors(
            string target,
            CancellationToken cancellationToken)
        {
            var results = new List<Rumor>();
            try
            {
                var client = gossiperClientFactory.Invoke(target);
                var pullTask = client.Pull(
                    new PullRequest(),
                    deadline: DateTime.UtcNow.AddMilliseconds(options.RoundIntervalMilliseconds / 2),
                    cancellationToken: cancellationToken);
                while (!cancellationToken.IsCancellationRequested
                    && await pullTask.ResponseStream.MoveNext(cancellationToken))
                {
                    results.Add(pullTask.ResponseStream.Current);
                }
            }
            catch (GrpcException ex)
            {
                logger.LogError(ex, "Failed to pull from {0} because {1}", target, ex.Message);
            }
            return results;
        }

        private void PassivePushRumors(object sender, PushRumorEventArgs e)
        {
            Task.Run(() => semaphore.ProtectAsync(() =>
            {
                foreach (var rumor in e.Rumors)
                {
                    ReceivePushedRumor(rumor);
                }
            }));
        }

        private void PassivePullRumors(object sender, PullRumorEventArgs e)
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
