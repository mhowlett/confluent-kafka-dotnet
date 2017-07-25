// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Examples.Security
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string topicName = args[0];

            var config = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", "localhost:9093" },
                { "security.protocol", "SSL" },
                { "ssl.ca.location", "/tmp/ca-cert" },
                { "debug", "security" }
            };

            var mutualAuthConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", "localhost:9093" },
                { "security.protocol", "SSL" },
                { "ssl.ca.location", "/tmp/ca-cert" },
                { "ssl.certificate.location", "/tmp/localhost_client.crt" },
                { "ssl.key.location", "/tmp/localhost_client.key" },
                { "debug", "security" }
            };

            using (var producer = new Producer<Null, string>(mutualAuthConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");

                producer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error}");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    var deliveryReport = producer.ProduceAsync(topicName, null, text);
                    deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }

                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
