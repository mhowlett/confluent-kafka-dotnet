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

#pragma warning disable xUnit1026

using System;
using System.Text;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.Admin;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Tests that log messages are received by OnLog on Producer, Consumer and AdminClients
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void LogEvent(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start LogEvent");

            var logCount = 0;

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                Debug = "all"
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Debug = "all"
            };

            var adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                Debug = "all"
            };

            DeliveryReport dr;

            using (var producer = new Producer(producerConfig))
            {
                producer.OnLog += (_, m)
                    => logCount += 1;

                dr = producer.ProduceAsync(singlePartitionTopic, new Message { Value = Serializers.UTF8("test value") }).Result;
                producer.Flush(TimeSpan.FromSeconds(10));
            }
            Assert.True(logCount > 0);

            logCount = 0;
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.OnLog += (_, m)
                    => logCount += 1;

                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));

                consumer.Consume(TimeSpan.FromSeconds(10));
            }
            Assert.True(logCount > 0);

            logCount = 0;
            using (var adminClient = new AdminClient(adminClientConfig))
            {
                adminClient.OnLog += (_, m)
                    => logCount += 1;

                var configResource = new ConfigResource { Name = "0", Type = ResourceType.Broker };
                adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Wait();
            }
            Assert.True(logCount > 0);

            logCount = 0;
            using (var producer = new Producer(producerConfig))
            using (var adminClient = new AdminClient(producer.Handle))
            {
                adminClient.OnLog += (_, m)
                    => logCount += 1;

                var configResource = new ConfigResource { Name = "0", Type = ResourceType.Broker };
                adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Wait();
            }
            Assert.True(logCount > 0);

            logCount = 0;
            using (var consumer = new Consumer(consumerConfig))
            using (var adminClient = new AdminClient(consumer.Handle))
            {
                adminClient.OnLog += (_, m)
                    => logCount += 1;

                var configResource = new ConfigResource { Name = "0", Type = ResourceType.Broker };
                adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Wait();
            }
            Assert.True(logCount > 0);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   LogEvent");
        }

    }
}
