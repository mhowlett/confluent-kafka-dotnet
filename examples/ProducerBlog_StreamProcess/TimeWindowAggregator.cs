using System;
using System.Threading;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public class Window
    {
        public DateTime WindowStart { get; set; }

        public int Count { get; set; }
    }

    public static class TimeWindowAggregator
    {
        static IProducer<DateTimeOffset, string> producer;
        static IConsumer<string, string> consumer;

        static void Process(ConsumeResult<Null, string> consumeResult, string outputTopic)
        {

        }

        public static void Run(
            string brokerAddress,
            string weblogTopic,
            string outputTopic,
            CancellationToken cancellationToken)
        {
            var cConfig = new ConsumerConfig
            {
                GroupId = "country-aggregator-group",
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

            using (producer = new ProducerBuilder<DateTime, >(pConfig).Build())
            using (consumer = new ConsumerBuilder<string, string>(cConfig).Build())
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
                            // talking to Frank, microservices 
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }

            Console.WriteLine("WeblogSimulator.Process method terminated.");
        }
    }
}
