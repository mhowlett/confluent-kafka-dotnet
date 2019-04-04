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
        
        static void Main(string[] args)
        {
            var brokerAddress = args[0];
            var weblogTopic = args[1];
            var outputTopic = args[2];

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

//            Task.Run(async () => await WeblogSimulator.Generate(brokerAddress, weblogTopic, cts.Token));

            StatelessProcessor.Run(brokerAddress, weblogTopic, outputTopic, cts.Token);
        }
    }
}
