using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Soak
{
    class Program
    {
        static string CreateStringValue(int length)
        {
            var result = "";
            for (int i=0; i<length/32+1; ++i)
            {
                result += Guid.NewGuid().ToString();
            }
            return result.Substring(0, length);
        }

        static void Produce()
        {
            var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };
            config.Set("compression.codec", "lz4");

            const int N = 500;
            const int M = 10000;
            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                for (int j=0; j<M; ++j)
                {
                    var drs = new List<Task<DeliveryResult<string, string>>>();
                    for (int i=0; i<N; ++i)
                    {
                        try
                        {
                            producer.ProduceAsync("soak", new Message<string, string> { Key = CreateStringValue(40), Value = CreateStringValue(2048) });
                            // Thread.Sleep(1);
                        }
                        catch (ProduceException<string, string> e)
                        {
                            if (e.Error.Code == ErrorCode.Local_QueueFull)
                            {
                                Thread.Sleep(100);
                            }
                        }
                    }
                    Task.WaitAll(drs.ToArray());
                    Console.WriteLine($"sent batch: {j}");
                }
            }
        }

        static void Consume()
        {
            Task.Run(() =>
            {
                var config = new ConsumerConfig { GroupId = "aa", BootstrapServers = "127.0.0.1:9092", Debug="all" };
                using (var consumer = new ConsumerBuilder<string, string>(config)
                    .SetRebalanceHandler((_, r) =>
                    {
                        Console.WriteLine(r.Partitions[0]);
                    })
                    .Build())
                {
                    consumer.Subscribe("soak");
                    long bytesReceived = 0;
                    while (true)
                    {
                    //    Console.WriteLine("as");
                        var c = consumer.Consume();
                        bytesReceived += c.Key.Length + c.Value.Length;
                    //    Console.WriteLine(bytesReceived);
                    }
                }
            });
        }

        static void SetupTopic()
        {
            var config = new AdminClientConfig { BootstrapServers = "127.0.0.1" };
            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification
                {
                    Name = "soak",
                    NumPartitions = 1,
                    ReplicationFactor = 1,
                    Configs = new Dictionary<string, string>
                    {
                        { "retention.bytes", "100000000" }
                    }
                } });
            }
        }

        static void Main(string[] args)
        {
            SetupTopic();
            Consume();
            Produce();
        }
    }
}
