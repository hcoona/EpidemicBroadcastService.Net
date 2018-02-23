using Google.Protobuf.WellKnownTypes;

namespace EpidemicBroadcastService
{
    public class RumorReceivedEventArgs
    {
        public Any Payload { get; set; }
    }
}
