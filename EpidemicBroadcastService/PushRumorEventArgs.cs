using System.Collections.Generic;

namespace EpidemicBroadcastService
{
    internal class PushRumorEventArgs
    {
        public IList<Rumor> Rumors { get; set; }
    }
}
