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
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_ClosedHandle(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "dotnet.producer.enable.background.poll", false }
            };
            var producer = new Producer(producerConfig);
            producer.Poll(TimeSpan.FromMilliseconds(10));
            producer.Dispose();
            Assert.Throws<ObjectDisposedException>(() => producer.Poll(TimeSpan.FromMilliseconds(10)));
        }

        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_ClosedHandle(string bootstrapServers, string topic, string partitionedTopic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers }
            };
            var consumer = new Consumer<byte[], byte[]>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            consumer.Poll(TimeSpan.FromMilliseconds(10));
            consumer.Dispose();
            Assert.Throws<ObjectDisposedException>(() => consumer.Poll(TimeSpan.FromMilliseconds(10)));
        }

        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void TypedProducer_ClosedHandle(string bootstrapServers, string topic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
            };
            var producer = new Producer<Null, Null>(producerConfig, null, null);
            producer.Flush(TimeSpan.FromMilliseconds(10));
            producer.Dispose();
            Assert.Throws<ObjectDisposedException>(() => producer.Flush(TimeSpan.FromMilliseconds(10)));
        }

        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void TypedConsumer_ClosedHandle(string bootstrapServers, string topic, string partitionedTopic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers }
            };
            var consumer = new Consumer<Null, Null>(consumerConfig, null, null);
            consumer.Poll(TimeSpan.FromMilliseconds(10));
            consumer.Dispose();
            Assert.Throws<ObjectDisposedException>(() => consumer.Poll(TimeSpan.FromMilliseconds(10)));
        }
    }
}
