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

using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.Examples.AvroSpecific;
using System;
using System.Linq;
using System.Threading;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkAvro
    {
        public static void ProduceConsume(string bootstrapServers, string schemaRegistryUrl, string topic, int messageCount)
        {
            using (var srClient = new CachedSchemaRegistryClient(Configuration.GetSchemaRegistryConfig(schemaRegistryUrl)))
            {
                var toProduce = new User
                {
                    name = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
                    favorite_color = "green",
                    favorite_number = 42
                };

                var autoEvent = new AutoResetEvent(false);
                var counter = messageCount;
                Action<DeliveryReport<Null, User>> dh = (DeliveryReport<Null, User> deliveryReport) 
                    => { if (--counter == 0) { autoEvent.Set(); } };

                DeliveryResult<Null, User> firstProduced;
                using (var producer = new Producer<Null, User>(Configuration.GetProducerConfig(bootstrapServers),
                    Serializers.Null, new AvroSerializer<User>(srClient)))
                {
                    // produce initial message, the only for which SR will be contacted.
                    firstProduced = producer.ProduceAsync(topic, new Message<Null, User> { Value = toProduce }).Result;

                    var startTime = DateTime.Now.Ticks;
                    for (int i=0; i<messageCount; ++i)
                    {
                        producer.BeginProduce(topic, new Message<Null, User> { Value = toProduce }, dh);
                    }
                    autoEvent.WaitOne();
                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Produced {messageCount} messages in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{messageCount / (duration/10000.0):F0}k msg/s");
                }

                using (var consumer = new Consumer<Null, User>(Configuration.GetConsumerConfig(bootstrapServers),
                    Deserializers.Null, new AvroDeserializer<User>(srClient)))
                {
                    consumer.OnPartitionsAssigned += (_, tps)
                        => consumer.Assign(tps.Select(tp => new TopicPartitionOffset(tp, firstProduced.Offset)));

                    consumer.Subscribe(topic);

                    // SR should not be contacted even on the first message, since the schema will already
                    // be cached from the produce calls. But still consume a message before starting the
                    // timer anyway to avoid including any consumer warmup time in results.
                    var first = consumer.Consume();

                    var startTime = DateTime.Now.Ticks;
                    var cnt = 0;
                    while (cnt < messageCount)
                    {
                        var cr = consumer.Consume(TimeSpan.FromSeconds(1));
                        if (cr != null) { cnt += 1; }
                    }
                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Consumed {messageCount} messages in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{messageCount / (duration/10000.0):F0}k msg/s");
                }
            }
        }
    }
}
