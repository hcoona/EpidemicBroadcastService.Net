namespace EpidemicBroadcastService
{
    public class EpidemicBroadcastServiceOption
    {
        public double MinimalSleepIntervalMilliseconds { get; set; }

        public double RoundIntervalMilliseconds { get; set; }

        public int FanOutNodeCountMax { get; set; }

        public int FanInNodeCountMax { get; set; }

        public int EstimatedNodeCountMin { get; set; }

        public double PushRoundsFactor { get; set; }

        public double PullRoundsFactor { get; set; }

        public double DeadRoundsFactor { get; set; }
    }
}
