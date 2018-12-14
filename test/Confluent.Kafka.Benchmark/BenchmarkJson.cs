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
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Text;
using System.Threading;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkJson
    {
        public class User
        {
            public string Name;
            public string FavoriteColor;
            public int FavoriteNumber;
        }

        public class JsonSerializer<T> : ISerializer<T>
        {
            public byte[] Serialize(T data, bool isKey, MessageMetadata messageAncillary, TopicPartition destination)
                => Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
        }

        public class JsonDeserializer<T> : IDeserializer<T>
        {
            public T Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageMetadata messageAncillary, TopicPartition source)
                => JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data));
        }

        public static void ProduceConsume(string bootstrapServers, string topic, int messageCount)
        {
            var autoEvent = new AutoResetEvent(false);
            var counter = messageCount;
            Action<DeliveryReport<Null, User>> dh = (DeliveryReport<Null, User> deliveryReport) 
                => { if (--counter == 0) { autoEvent.Set(); } };

            var toProduce = new User
            {
                Name = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
                FavoriteColor = "green",
                FavoriteNumber = 42
            };

            DeliveryResult<Null, User> firstProduced;
            using (var producer = new Producer<Null, User>(
                Configuration.GetProducerConfig(bootstrapServers), Serializers.Null, new JsonSerializer<User>()))
            {
                // produce a message before starting the timer to avoid including any warmup time in result.
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

            using (var consumer = new Consumer<Null, User>(
                Configuration.GetConsumerConfig(bootstrapServers), Deserializers.Null, new JsonDeserializer<User>()))
            {
                consumer.OnPartitionsAssigned += (_, tps) =>
                    consumer.Assign(tps.Select(tp => new TopicPartitionOffset(tp, firstProduced.Offset)));

                consumer.Subscribe(topic);

                // Don't start timing until the first message is received to avoid any consumer warmup delay.
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
