using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.Extensions.Streaming.Processors
{
    /// <summary>
    ///     A very simple processor type that repeatedly calls the
    ///     specified <see cref="GeneratorProcessor.Function" />
    ///     (synchronously). Your function should be self rate-
    ///     limiting (e.g. by calling <see cref="System.Threading.Thread.Sleep(TimeSpan)" />).
    /// </summary>
    public class GeneratorProcessor<TOutKey, TOutValue>
    {
        public string BootstrapServers { get; set; }

        public string OutputTopic { get; set; }

        public Func<Message<TOutKey, TOutValue>> Function { get; set; }

        public ISerializer<TOutKey> KeySerializer { get; set; }

        public ISerializer<TOutValue> ValueSerializer { get; set; }

        public void Start(CancellationToken cancellationToken)
        {
            var builder = new ProducerBuilder<TOutKey, TOutValue>(
                new ProducerConfig
                {
                    BootstrapServers = BootstrapServers,
                    EnableIdempotence = true
                });
            if (KeySerializer != null)
            {
                builder.SetKeySerializer(KeySerializer);
            }
            if (ValueSerializer != null)
            {
                builder.SetValueSerializer(ValueSerializer);
            }
            builder.SetErrorHandler((_, e) =>
            {
                // TODO:
            });

            Action<DeliveryReport<TOutKey, TOutValue>> dh = dr =>
            {
                // Probably quit if there's a problem.
            };

            using (var producer = builder.Build())
            {
                try
                {
                    while (true)
                    {
                        producer.Produce(OutputTopic, Function(), dh);
                    }
                }
                catch (OperationCanceledException) { }
            }
        }

        public Task Run(CancellationToken cancellationToken)
            => Task.Run(() => Start(cancellationToken));
    }
}
