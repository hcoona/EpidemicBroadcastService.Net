using System;
using System.Collections.Generic;

namespace EpidemicBroadcastService.Extensions
{
    internal static class EnumerableExtensions
    {
        // Algorithm R
        // http://www.cs.umd.edu/~samir/498/vitter.pdf
        public static IEnumerable<T> RandomTake<T>(this IEnumerable<T> source, Random random, int count)
        {
            var result = new List<T>(count);
            using (var enumerator = source.GetEnumerator())
            {
                int i = 0;
                while (enumerator.MoveNext())
                {
                    result.Add(enumerator.Current);
                    i++;
                    if (i == count) break;
                }

                if (i == count)
                {
                    while (enumerator.MoveNext())
                    {
                        var j = random.Next(i);
                        if (j < count)
                        {
                            result[j] = enumerator.Current;
                        }
                    }
                }
            }
            return result;
        }
    }
}
