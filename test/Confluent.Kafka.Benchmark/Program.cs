// Copyright 2016-2017 Confluent Inc.
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
// Refer to LICENSE for more information.

using System;
using Mono.Options;


namespace Confluent.Kafka.Benchmark
{
    public class Program
    {
        private static void WriteHelp(OptionSet options)
        {
            Console.WriteLine("Options: ");
            options.WriteOptionDescriptions(Console.Out);
        }

        public static void Main(string[] args)
        {
            string serializationFormat = null;
            string bootstrapServers = null;
            string schemaRegistryUrl = null;
            string topic = null;
            int headerCount = 0;
            int messageCount = 1_000_000;

            var p = new OptionSet
            {
                { "f=", "Serialization format <bytes|avro|protobuf|string>.", v => serializationFormat = v },
                { "b=", "Comma separated list of brokers.", v => bootstrapServers = v },
                { "s=", "Schema Registry url.", v => schemaRegistryUrl = v },
                { "t=", "Kafka topic", v => topic = v },
                { "h=", "Header count (default 0)", v => headerCount = int.Parse(v) },
                { "n=", "Number of messages to produce/consume (default 1M)", v => messageCount = int.Parse(v) }
            };

            if (args.Length == 0)
            {
                WriteHelp(p);
                Environment.Exit(0);
            }

            try
            {
                p.Parse(args);
            }
            catch (Exception)
            {
                WriteHelp(p);
                Environment.Exit(1);
            }

            if (bootstrapServers == null) { Console.WriteLine("broker must be specified."); Environment.Exit(1); }
            if (topic == null) { Console.WriteLine("topic must be specified"); Environment.Exit(1); }
            if (serializationFormat == null) { Console.WriteLine("serialization format must be specified"); Environment.Exit(1); }

            switch (serializationFormat)
            {
                case "bytes":
                    const int NUMBER_OF_TESTS = 1;
                    BenchmarkBytesProducer.TaskProduce(bootstrapServers, topic, messageCount, headerCount, NUMBER_OF_TESTS);
                    var firstMessageOffset = BenchmarkBytesProducer.DeliveryHandlerProduce(bootstrapServers, topic, messageCount, headerCount, NUMBER_OF_TESTS);
                    BenchmarkBytesConsumer.Consume(bootstrapServers, topic, firstMessageOffset, messageCount, headerCount, NUMBER_OF_TESTS);
                    break;
                case "avro":
                    BenchmarkAvro.ProduceConsume(bootstrapServers, schemaRegistryUrl, topic, messageCount);
                    break;
                case "protobuf":
                    BenchmarkProtobuf.ProduceConsume(bootstrapServers, topic, messageCount);
                    break;
                case "string":
                    BenchmarkString.ProduceConsume(bootstrapServers, topic, messageCount);
                    break;
                case "json":
                    BenchmarkJson.ProduceConsume(bootstrapServers, topic, messageCount);
                    break;
                default:
                    Console.WriteLine($"unknown serialization type {serializationFormat}");
                    break;
            }
        }
    }
}
