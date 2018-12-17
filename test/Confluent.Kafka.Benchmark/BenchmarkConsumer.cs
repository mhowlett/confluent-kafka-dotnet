// Copyright 2016-2018 Confluent Inc.
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
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkConsumer
    {
        public static void BenchmarkConsumerImpl(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nHeaders)
        {
            var nReportInterval = nMessages / 10;

            var consumerConfig = new ConsumerConfig
            {
                GroupId = "benchmark-consumer-group",
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                ConsumeResultFields = nHeaders == 0 ? "none" : "headers",
                QueuedMinMessages = 10000000
            };

            using (var consumer = new Consumer<Ignore, Ignore>(consumerConfig))
            {
                Console.WriteLine($"{consumer.Name} consuming from {topic}:");

                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, firstMessageOffset) });

                var stopwatch = new Stopwatch();

                // consume 1 message before starting the timer to avoid including potential one-off delays.
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                if (record == null) { throw new Exception("First record was not consumed in a timely manner."); }

                stopwatch.Start();

                var cnt = 0;
                long lastElapsedMs = 0;
                while (cnt < nMessages-1)
                {
                    record = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (record == null)
                    {
                        throw new Exception("Local consumer queue is starved, this is unexpected.");
                    }
                    cnt += 1;

                    if (cnt % nReportInterval == 0)
                    {
                        var elapsedMs = stopwatch.ElapsedMilliseconds;
                        Console.WriteLine($"  Consumed {nReportInterval} messages in {elapsedMs - lastElapsedMs:F0}ms");
                        Console.WriteLine($"  {nReportInterval / (elapsedMs - lastElapsedMs):F0}k msg/s");
                        lastElapsedMs = elapsedMs;
                    }
                }

                var durationMs = stopwatch.ElapsedMilliseconds;

                Console.WriteLine($"  Total:");
                Console.WriteLine($"    Consumed {nMessages-1} messages in {durationMs:F0}ms");
                Console.WriteLine($"    {(nMessages-1) / durationMs:F0}k msg/s");
            }
        }

        public static void Consume(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nHeaders)
            => BenchmarkConsumerImpl(bootstrapServers, topic, firstMessageOffset, nMessages, nHeaders);
    }
}
