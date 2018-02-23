using System.Collections.Generic;

namespace EpidemicBroadcastService
{
    internal class PullRumorEventArgs
    {
        public IEnumerable<Rumor> Rumors { get; set; }
    }
}
