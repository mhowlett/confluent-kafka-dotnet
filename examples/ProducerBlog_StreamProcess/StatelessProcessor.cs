using System;
using System.Threading;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public static class StatelessProcessor
    {
        static IProducer<string, string> producer;
        static IConsumer<Null, string> consumer;

        static void Process(ConsumeResult<Null, string> consumeResult, string outputTopic)
        {
            while (true)
            {
                try
                {
                    var logline = consumeResult.Value;
                    var firstSpaceIndex = logline.IndexOf(' ');
                    var ip = logline.Substring(0, firstSpaceIndex);
                    var country = MockGeoLookup.GetCountryFromIP(ip);
                    var anonymizedLogline = logline.Substring(firstSpaceIndex);

                    producer.BeginProduce(
                        outputTopic,
                        new Message<string, string> { Key = country, Value = anonymizedLogline },
                        dr =>
                        {
                            // closure!
                            consumer.StoreOffset(consumeResult);
                        });
                }
                catch (ProduceException<long, String> ex)
                {
                    if (ex.Error.Code == ErrorCode.Local_QueueFull)
                    {
                        producer.Poll(TimeSpan.FromSeconds(1));
                    }
                    continue;
                }

                break;
            }
        }

        public static void Run(
            string brokerAddress,
            string weblogTopic,
            string outputTopic,
            CancellationToken cancellationToken)
        {
            var cConfig = new ConsumerConfig
            {
                GroupId = "weblog-example",
                BootstrapServers = brokerAddress,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerAddress,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none"
            };

            using (producer =
                new ProducerBuilder<string, string>(pConfig)
                    .SetErrorHandler((_, e) =>
                    {
                        // idempotent fatal error.
                    })
                    .Build())
            using (consumer =
                new ConsumerBuilder<Null, string>(cConfig)
                    .Build())
            {
                consumer.Subscribe(weblogTopic);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cancellationToken);
                            Process(cr, outputTopic);
                        }
                        catch (ConsumeException ex)
                        {
                            if (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
                            {
                                // bad data.
                                continue;
                            }
                            // what else?
                            // microservices we want to fail fast and restart the microservice.
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }

            Console.WriteLine("WeblogSimulator.Process method terminated.");
        }
    }
}
