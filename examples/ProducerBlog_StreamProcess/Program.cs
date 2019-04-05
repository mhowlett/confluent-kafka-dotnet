using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Bogus;
using Bogus.DataSets;


namespace ProducerBlog_StatelessProcessing
{
    class Program
    {
        async static Task RecreateTopicsAsync(
            string brokerAddress,
            string weblogTopic,
            string piiCompliantTopic,
            string aggregatedTopic,
            string windowOffsetTopic)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokerAddress }).Build())
            {
                try
                {
                    await adminClient.DeleteTopicsAsync(new [] { weblogTopic, piiCompliantTopic, aggregatedTopic, windowOffsetTopic });
                }
                catch (DeleteTopicsException ex)
                {
                    foreach (var r in ex.Results)
                    {
                        if (r.Error.Code != ErrorCode.UnknownTopicOrPart)
                        {
                            throw;
                        }
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(2));

                await adminClient.CreateTopicsAsync(new []
                    {
                        // set replication factor to 1 so can work on single broker test cluster. typically set to 3.
                        new TopicSpecification { Name = weblogTopic, NumPartitions = 24, ReplicationFactor = 1 },
                        new TopicSpecification { Name = piiCompliantTopic, NumPartitions = 24, ReplicationFactor = 1 },
                        new TopicSpecification { Name = aggregatedTopic, NumPartitions = 2, ReplicationFactor = 1 },
                        new TopicSpecification { Name = windowOffsetTopic, NumPartitions = 1, ReplicationFactor = 1 }
                    });
            }
        }

        async static Task Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.WriteLine("usage: .. <bootstrap servers> <weblog topic> <processed topic> <aggregated topic> <window offset topic>");
                Environment.Exit(1);
            }

            var brokerAddress = args[0];
            var weblogTopic = args[1];
            var piiCompliantTopic = args[2];
            var countryCountTopic = args[3];
            var windowOffsetTopic = args[4];

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            await RecreateTopicsAsync(brokerAddress, weblogTopic, piiCompliantTopic, countryCountTopic, windowOffsetTopic);

            // In a production system, each task (thread) below would correspond
            // to a separate process.
            var microservices = new List<Task>();

            // generate some fake weblog data.
            microservices.Add(Task.Run(async () => await WeblogSimulator.Generate(brokerAddress, weblogTopic, cts.Token)));

            // (mock) geoip lookup, remove pii information (IP address), and repartition by country.
            microservices.Add(Task.Run(() => StatelessProcessor.Run(brokerAddress, weblogTopic, piiCompliantTopic, 1, cts.Token)));
            // microservices.Add(Task.Run(() => StatelessProcessor.Run(brokerAddress, weblogTopic, piiCompliantTopic, 2, cts.Token)));

            // count the number of hits per country in 1hr time windows.
            microservices.Add(Task.Run(() => TimeWindowAggregator.Run(brokerAddress, piiCompliantTopic, countryCountTopic, windowOffsetTopic, 1, cts.Token)));
            // microservices.Add(Task.Run(() => TimeWindowAggregator.Run(brokerAddress, piiCompliantTopic, aggregatedTopic, 2, cts.Token)));

            var result = await Task.WhenAny(microservices);

            if (result.IsFaulted)
            {
                throw result.Exception;
            }
        }
    }
}
