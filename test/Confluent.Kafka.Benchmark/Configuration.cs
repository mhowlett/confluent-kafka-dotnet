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
using System;
using System.Linq;
using System.Threading;


namespace Confluent.Kafka.Benchmark
{
    /// <summary>
    ///     Mirrors the librdkafka performance test example.
    /// </summary>
    public static class Configuration
    {
        public static ProducerConfig GetProducerConfig(string bootstrapServers)
        {
            return new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                QueueBufferingMaxMessages = 2_000_000,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 500 ,
                LingerMs = 100,
                DeliveryReportFields = "none"
            };
        }

        public static SchemaRegistryConfig GetSchemaRegistryConfig(string schemaRegistryUrl)
        {
            return new SchemaRegistryConfig
            {
                SchemaRegistryUrl = schemaRegistryUrl
            };
        }

        public static ConsumerConfig GetConsumerConfig(string bootstrapServers)
        {
            return new ConsumerConfig
            {
                GroupId = "benchmark-consumer-group",
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                ConsumeResultFields = "none"
            };
        }
    }
}
