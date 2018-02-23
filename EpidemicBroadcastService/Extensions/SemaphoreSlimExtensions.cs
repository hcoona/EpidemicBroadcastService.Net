using System;
using System.Threading;
using System.Threading.Tasks;

namespace EpidemicBroadcastService.Extensions
{
    internal static class SemaphoreSlimExtensions
    {
        public static void Protect(
            this SemaphoreSlim semaphore,
            Action action)
        {
            semaphore.Wait();
            try
            {
                action();
            }
            finally
            {
                semaphore.Release();
            }
        }

        public static async Task ProtectAsync(
            this SemaphoreSlim semaphore,
            Action action,
            CancellationToken cancellationToken = default)
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                action();
            }
            finally
            {
                semaphore.Release();
            }
        }

        public static async Task ProtectAsync(
            this SemaphoreSlim semaphore,
            Func<CancellationToken, Task> actionAsync,
            CancellationToken cancellationToken = default)
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                await actionAsync(cancellationToken);
            }
            finally
            {
                semaphore.Release();
            }
        }

        public static async Task<T> ProtectAsync<T>(
            this SemaphoreSlim semaphore,
            Func<T> action,
            CancellationToken cancellationToken = default)
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                return action();
            }
            finally
            {
                semaphore.Release();
            }
        }

        public static async Task<T> ProtectAsync<T>(
            this SemaphoreSlim semaphore,
            Func<CancellationToken, Task<T>> actionAsync,
            CancellationToken cancellationToken = default)
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                return await actionAsync(cancellationToken);
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}
