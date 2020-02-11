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

using Xunit;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.Kafka.Examples.Protobuf;

namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test producing/consuming using both regular and Avro serializers.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProtobufTest(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers
            };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, NestedOuter.Types.NestedMid2.Types.NestedLower>(producerConfig)
                    .SetValueSerializer(new ProtobufSerializer<NestedOuter.Types.NestedMid2.Types.NestedLower>(schemaRegistry))
                    .Build())
            {
                var u = new NestedOuter.Types.NestedMid2.Types.NestedLower();
                u.Field2 = "field_2_value";
                producer.ProduceAsync(topic.Name, new Message<string, NestedOuter.Types.NestedMid2.Types.NestedLower> { Key = "test1", Value = u }).Wait();

                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

                using (var consumer =
                    new ConsumerBuilder<string, NestedOuter.Types.NestedMid2.Types.NestedLower>(consumerConfig)
                        .SetValueDeserializer(new ProtobufDeserializer<NestedOuter.Types.NestedMid2.Types.NestedLower>()
                            .AsSyncOverAsync())
                        .Build())
                {
                    consumer.Subscribe(topic.Name);
                    var cr = consumer.Consume();
                }
            }

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, Outer2>(producerConfig)
                    .SetValueSerializer(new ProtobufSerializer<Outer2>(schemaRegistry))
                    .Build())
            {
                var u = new Outer2();
                u.FieldB = "field_b_value";
                producer.ProduceAsync(topic.Name, new Message<string, Outer2> { Key = "test2", Value = u }).Wait();
            }

        }
    }
}
