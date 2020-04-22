// Copyright 2020 Confluent Inc.
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

#pragma warning disable xUnit1026

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test <see cref="Consumer.Assign" /> behaviour with Offset.End
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Assign2(string bootstrapServers)
        {
            LogToFile("start Consumer_Assign2");

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer =
                new ProducerBuilder<Null, string>(
                        new ProducerConfig
                        {
                            BootstrapServers = bootstrapServers,
                            Acks = Acks.None
                        })
                    .Build())
            using (var consumer =
                new ConsumerBuilder<Null, string>(
                        new ConsumerConfig
                        {
                            GroupId = Guid.NewGuid().ToString(),
                            BootstrapServers = bootstrapServers,
                            EnableAutoCommit = false
                        })
                    .Build())
            {
                var runningLockObj = new object();
                bool running = true;
                var produceTask = Task.Run(
                    async () =>
                    {
                        while (true)
                        {
                            lock (runningLockObj) { if (!running) break; }
                            await producer.ProduceAsync(
                                topic.Name,
                                new Message<Null, string>
                                {
                                    Value = DateTime.UtcNow.ToString()
                                });
                            await Task.Delay(1000);
                        }
                    });

                void Consume(int n)
                {
                    for (int i = 0; i < n; i++)
                    {
                        Thread.Sleep(1000);
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(0.5));
                        Console.WriteLine($"Consumed: {consumeResult?.Offset} {consumeResult?.Message.Value ?? "nothing"}");
                    }
                }

                consumer.Assign(new TopicPartitionOffset(new TopicPartition(topic.Name, 0), Offset.End));
                Console.WriteLine("Subscribed.");

                Consume(5);

                consumer.Assign(new List<TopicPartition>());
                Console.WriteLine("Unsubscribed.");

                Consume(5);

                consumer.Assign(new TopicPartitionOffset(new TopicPartition(topic.Name, 0), Offset.End));
                Console.WriteLine("Subscribed.");

                Consume(5);

                lock (logLockObj) { running = false; }
                produceTask.Wait();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Assign2");
        }
    }
}
