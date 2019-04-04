using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Bogus;
using Bogus.DataSets;


namespace ProducerBlog_StatelessProcessing
{
    class Program
    {
        async static Task Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("usage: .. <bootstrap servers> <weblog topic> <processed topic>");
                Environment.Exit(1);
            }

            var brokerAddress = args[0];
            var weblogTopic = args[1];
            var piiCompliantTopic = args[2];
            var aggregatedTopic = args[3];

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            // generate fake weblog data.
            _ = Task.Run(async () => await WeblogSimulator.Generate(brokerAddress, weblogTopic, cts.Token));
            
            // (mock) geoip lookup, remove pii information (IP address), and repartition by country.
            _ = Task.Run(() => StatelessProcessor.Run(brokerAddress, weblogTopic, piiCompliantTopic, cts.Token));

            // count the number of hits per country in 1hr time windows.
            _ = Task.Run(() => TimeWindowAggregator.Run(brokerAddress, piiCompliantTopic, aggregatedTopic, cts.Token));
            
            await Task.Delay(TimeSpan.MaxValue, cts.Token);
        }
    }
}
