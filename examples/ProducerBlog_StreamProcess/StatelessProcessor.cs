using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public static class StatelessProcessor
    {
        static IProducer<string, string> producer;
        static IConsumer<Null, string> consumer;

        async static Task Process(ConsumeResult<Null, string> consumeResult, string outputTopic)
        {
            while (true)
            {
                try
                {
                    var logline = consumeResult.Value;
                    var firstSpaceIndex = logline.IndexOf(' ');
                    if (firstSpaceIndex < 0)
                    {
                        throw new Exception("");
                    }
                    var ip = logline.Substring(0, firstSpaceIndex);
                    var country = await MockGeoLookup.GetCountryFromIPAsync(ip);
                    var loglineWithoutIP = logline.Substring(firstSpaceIndex+1);
                    var dateStart = loglineWithoutIP.IndexOf('[');
                    var dateEnd = loglineWithoutIP.IndexOf(']');
                    if (dateStart < 0 || dateEnd < 0 || dateEnd < dateStart)
                    {
                        throw new Exception("");
                    }
                    var requestInfo = loglineWithoutIP.Substring(dateEnd);
                    
                    producer.BeginProduce(
                        outputTopic,
                        new Message<string, string> { Key = country, Value = requestInfo, Timestamp = new Timestamp(DateTime.UtcNow, TimestampType.CreateTime) },
                        dr =>
                        {
                            Console.WriteLine($"wrote: {requestInfo}");
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
            int instanceId,
            CancellationToken cancellationToken)
        {
            var cConfig = new ConsumerConfig
            {
                ClientId = $"pii-country-proc-consumer-{instanceId}",
                GroupId = "weblog-example",
                BootstrapServers = brokerAddress,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            var pConfig = new ProducerConfig
            {
                ClientId = $"pii-country-proc-producer-{instanceId}",
                BootstrapServers = brokerAddress,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none"
            };


            bool processedMessage = false;

            using (producer =
                new ProducerBuilder<string, string>(pConfig)
                    .SetErrorHandler((_, e) =>
                    {
                        if (e.IsFatal)
                        {
                            // can these exceptions be caught?
                            throw new KafkaException(e);

                            // idempotent fatal error.
                            //   - another case if different replicas are deleting log segments due to 
                            //     retention at different speeds. low watermark offset changes on differnt replicas.
                            //   - what if bad message, then maybe not guaraneed 1m not produced.
                        }

                        if (e.Code == ErrorCode.Local_AllBrokersDown ||
                            e.Code == ErrorCode.Local_Authentication)
                        {
                            if (!processedMessage)
                            {
                                throw new KafkaException(e);
                            }
                        }

                        // anything else best thought of as informational.
                        // perhaps rates - if there's problems then quit? 
                    })
                    .Build())

            using (consumer =
                new ConsumerBuilder<Null, string>(cConfig)
                    .SetErrorHandler((_, e) =>
                    {
                        if (e.Code == ErrorCode.Local_AllBrokersDown ||
                            e.Code == ErrorCode.Local_Authentication)
                        {
                            if (!processedMessage)
                            {
                                throw new KafkaException(e);
                            }
                        }
                    })
                    // failure to produce offsets.
                    // measure eveything by rte
                    //  consumption rate drops below 1 ten something wrong
                    //  also what if # commit failures starting to reach input rae, can't comit.
                    //  offset commits on own connection.
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
                            processedMessage = true;
                        }
                        catch (ConsumeException ex)
                        {
                            if (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
                            {
                                continue;
                            }

                            // what else?
                            //  - group coordinator error.
                            //  - any error you don't recognise should be ignored.
                            //  - microservices we want to fail fast and restart the microservice.
                            // I think we want to ignore:
                            continue;
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }

            Console.WriteLine("WeblogSimulator.Process method terminated.");
        }
    }
}
