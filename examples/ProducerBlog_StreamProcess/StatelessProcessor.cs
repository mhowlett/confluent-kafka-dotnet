using System;
using System.Threading;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public static class StatelessProcessor
    {
        static IProducer<long, string> producer;
        static IConsumer<Null, string> consumer;

        static void Process(ConsumeResult<Null, string> consumeResult, string outputTopic)
        {
            Console.WriteLine("1");
            consumer.StoreOffset(consumeResult);
            Console.WriteLine("2");

            Console.WriteLine(consumeResult.Value);
            while (true)
            {
                try
                {
                    producer.BeginProduce(
                        outputTopic,
                        new Message<long, string> { Key=0, Value = consumeResult.Value },
                        dr =>
                        {
                            Console.WriteLine("storing " + consumeResult.Offset);
                            // consumer.StoreOffset(consumeResult);
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
                // Segfault if don't specify this.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                ConsumeResultFields = "none"
            };

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerAddress,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none"
            };

            using (producer = new ProducerBuilder<long, string>(pConfig).Build())
            using (consumer = new ConsumerBuilder<Null, string>(cConfig).Build())
            {
                consumer.Subscribe(weblogTopic);

                try
                {
                    while (true)
                    {
                        var cr = consumer.Consume(cancellationToken);
                        Process(cr, outputTopic);
                    }
                }
                catch (OperationCanceledException ex)
                {
                    // 
                }
            }

            Console.WriteLine("WeblogSimulator thread exiting.");
        }
    }
}
