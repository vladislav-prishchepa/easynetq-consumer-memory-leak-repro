using System;
using System.Threading;
using System.Threading.Tasks;
using EasyNetQ;
using EasyNetQ.Topology;

namespace StreamConsumer
{
    internal static class Program
    {
        private static readonly CancellationTokenSource StoppingCts = new();
        private static readonly CancellationToken StoppingToken = StoppingCts.Token;
        private static readonly byte[] Message = new byte[4];
        private static readonly MessageProperties MessageProperties = new();

        private static async Task Main()
        {
            Console.CancelKeyPress += (_, args) =>
            {
                args.Cancel = true;
                StoppingCts.Cancel();
            };

            try
            {
                using var bus = RabbitHutch.CreateBus("host=localhost;username=guest;password=guest;virtualHost=/;timeout=10").Advanced;

                var exchange = await bus.ExchangeDeclareAsync("memory_leak", ExchangeType.Direct, false, false, StoppingToken);
                
                while (!StoppingToken.IsCancellationRequested)
                {
                    var routingKey = Guid.NewGuid().ToString("N");
                    var queue = await bus.QueueDeclareAsync($"memory_leak_{routingKey}", false, true, true, StoppingToken);
                    var binding = await bus.BindAsync(exchange, queue, routingKey, StoppingToken);
                    
                    var receivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

                    await using var cancellationRegistration = StoppingToken.UnsafeRegister(
                        static arg => ((TaskCompletionSource)arg!).TrySetResult(),
                        receivedTcs);

                    using (bus.Consume(queue, (_, _, _) =>
                    {
                        receivedTcs.TrySetResult();
                    }))
                    {
                        await bus.PublishAsync(exchange, routingKey, true, MessageProperties, Message, StoppingToken);
                        await receivedTcs.Task;
                    }

                    await bus.UnbindAsync(binding, StoppingToken);
                    await bus.QueueDeleteAsync(queue, cancellationToken: StoppingToken);
                }
            }
            catch (OperationCanceledException) when (StoppingToken.IsCancellationRequested)
            {
            }
        }
    }
}
