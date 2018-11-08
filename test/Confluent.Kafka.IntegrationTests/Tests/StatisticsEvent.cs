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
using System.Threading;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.Admin;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Tests that log messages are received by OnStatistics on Producer, Consumer and AdminClients
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void StatisticsEvent(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start StatisticsEvent");

            var statsCount = 0;

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                StatisticsIntervalMs = 1000  // granularity is 1000ms.
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                StatisticsIntervalMs = 1000
            };

            var adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                Debug = "all",
                StatisticsIntervalMs = 1000
            };

            using (var producer = new Producer(producerConfig))
            {
                producer.OnStatistics += (_, s)
                    => statsCount += 1;

                for (int i=0; i<50; ++i) // more than enough to consume back later in test.
                {
                    producer.ProduceAsync(singlePartitionTopic, new Message { Value = Serializers.UTF8("test value") });
                }
                
                Thread.Sleep(TimeSpan.FromSeconds(2));  // background poll will ensure OnStatistics is called in this period.
                producer.Flush(TimeSpan.FromSeconds(10));
            }
            Assert.True(statsCount > 0);

            statsCount = 0;
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.OnStatistics += (_, m)
                    => statsCount += 1;

                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                consumer.Consume(TimeSpan.FromSeconds(10));
                Thread.Sleep(2);

                // consumer has no background poll, so OnStatistics won't fire without this.
                consumer.Consume(TimeSpan.FromSeconds(10));

                // OnStatistics is NOT called as a side-effect of close.
                consumer.Close(); 
            }
            Assert.True(statsCount > 0);

            statsCount = 0;
            using (var adminClient = new AdminClient(adminClientConfig))
            {
                adminClient.OnStatistics += (_, m)
                    => statsCount += 1;

                var configResource = new ConfigResource { Name = "0", Type = ResourceType.Broker };
                adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Wait();

                // background polling in wrapped producer will ensure OnStatistics is called.
                Thread.Sleep(TimeSpan.FromSeconds(2)); 
            }
            Assert.True(statsCount > 0);

            statsCount = 0;
            using (var producer = new Producer(producerConfig))
            using (var adminClient = new AdminClient(producer.Handle))
            {
                adminClient.OnStatistics += (_, m)
                    => statsCount += 1;

                var configResource = new ConfigResource { Name = "0", Type = ResourceType.Broker };
                adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Wait();

                // background poll of producer will ensure OnStatistics is called.
                Thread.Sleep(TimeSpan.FromSeconds(2));
            }
            Assert.True(statsCount > 0);

            statsCount = 0;
            using (var consumer = new Consumer(consumerConfig))
            using (var adminClient = new AdminClient(consumer.Handle))
            {
                adminClient.OnStatistics += (_, m)
                    => statsCount += 1;

                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                consumer.Consume(TimeSpan.FromSeconds(10));

                var configResource = new ConfigResource { Name = "0", Type = ResourceType.Broker };
                adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Wait();
                Thread.Sleep(TimeSpan.FromSeconds(2));

                // consumer has no background poll, so OnStatistics won't fire without this.
                consumer.Consume(TimeSpan.FromSeconds(10));

                // OnStatistics is NOT called as a side-effect of close.
                consumer.Close();
            }
            Assert.True(statsCount > 0);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   StatisticsEvent");
        }

    }
}
