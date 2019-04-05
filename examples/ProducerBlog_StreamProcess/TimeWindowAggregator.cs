using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public class Window
    {
        public long WindowId { get; set; }

        public string Country { get; set; }

        public class Serializer : ISerializer<Window>
        {
            public byte[] Serialize(Window data, SerializationContext context)
            {
                // TODO: optimize.
                var bs1 = Confluent.Kafka.Serializers.Int64.Serialize(data.WindowId, context);
                var bs2 = Encoding.UTF8.GetBytes(data.Country);
                var result = new byte[bs1.Length + bs2.Length];
                bs1.CopyTo(result, 0);
                bs2.CopyTo(result, bs1.Length);
                return result;
            }
        }
    }

    public static class TimeWindowAggregator
    {
        static int BucketSizeSeconds = 30;
        static int CommitDelaySeconds = 5;


        static TimeWindowCommitManager commitManager;


        static IProducer<Window, long> producer;
        static IConsumer<string, string> consumer;

        static Dictionary<long, Dictionary<string, int>> countryCounts;

        static void Process(ConsumeResult<string, string> consumeResult, string outputTopic)
        {
            var timeBucketId = consumeResult.Timestamp.UnixTimestampMs / (int)TimeSpan.FromSeconds(BucketSizeSeconds).TotalMilliseconds;
            
            // bucket size is T seconds.
            // close off bucket when receive first message t1 seconds after bucket close.
        }

        public static void Run(
            string brokerAddress,
            string weblogTopic,
            string outputTopic,
            string windowOffsetTopic,
            int instanceId,
            CancellationToken cancellationToken)
        {
            var cConfig = new ConsumerConfig
            {
                ClientId = $"country-count-aggregator-{instanceId}",
                GroupId = "country-count-aggregator-group",
                BootstrapServers = brokerAddress,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var pConfig = new ProducerConfig
            {
                ClientId = $"country-count-aggregator-{instanceId}",
                BootstrapServers = brokerAddress,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none"
            };


            using (commitManager = new TimeWindowCommitManager(brokerAddress, windowOffsetTopic))

            using (producer =
                new ProducerBuilder<Window, long>(pConfig)
                    .SetKeySerializer(new Window.Serializer())
                    .SetErrorHandler((_, e) =>
                    {
                        
                    })
                    .Build())
            
            using (consumer =
                new ConsumerBuilder<string, string>(cConfig)
                    .SetPartitionsAssignedHandler((_, p) =>
                    {
                        
                    })
                    .SetErrorHandler((_, e) =>
                    {
                        
                    })
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
                                continue;
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }

            Console.WriteLine("WeblogSimulator.Process method terminated.");
        }
    }
}
