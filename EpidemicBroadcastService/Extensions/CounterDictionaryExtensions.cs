using System.Collections.Generic;

namespace EpidemicBroadcastService.Extensions
{
    internal static class CounterDictionaryExtensions
    {
        public static int IncreaseCounter<T>(this IDictionary<T, int> counterDictionary, T key)
        {
            int value;
            if (counterDictionary.TryGetValue(key, out var counter))
            {
                value = counter + 1;
            }
            else
            {
                value = 1;
            }
            return counterDictionary[key] = value;
        }

        public static int GetOrDefault<T>(this IDictionary<T, int> counterDictionary, T key, int @default = default)
        {
            if (counterDictionary.TryGetValue(key, out var counter))
            {
                return counter;
            }
            else
            {
                return @default;
            }
        }
    }
}
