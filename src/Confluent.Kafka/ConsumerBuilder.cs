// Copyright 2018 Confluent Inc.
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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A builder class for <see cref="Consumer" /> instances.
    /// </summary>
    public class ConsumerBuilder
    {
        internal IEnumerable<KeyValuePair<string, string>> config;
        internal Action<Consumer, Error> errorHandler;
        internal Action<Consumer, LogMessage> logHandler;
        internal Action<Consumer, string> statsHandler;
        internal Action<Consumer, List<TopicPartition>> partitionAssignmentHandler;
        internal Action<Consumer, List<TopicPartition>> partitionAssignmentRevokedHandler;
        internal Action<Consumer, CommittedOffsets> offsetsCommittedHandler;

        internal ConsumerBase.Config ConstructBaseConfig(Consumer consumer)
        {
            return new ConsumerBase.Config
            {
                config = config,
                errorHandler = this.errorHandler == null
                    ? default(Action<Error>) // using default(...) rather than null (== default(...)) so types can be inferred.
                    : error => this.errorHandler(consumer, error),
                logHandler = this.logHandler == null
                    ? default(Action<LogMessage>)
                    : logMessage => this.logHandler(consumer, logMessage),
                statsHandler = this.statsHandler == null
                    ? default(Action<string>)
                    : stats => this.statsHandler(consumer, stats),
                partitionAssignmentRevokedHandler = this.partitionAssignmentRevokedHandler == null
                    ? default(Action<List<TopicPartition>>)
                    : partitions => this.partitionAssignmentRevokedHandler(consumer, partitions),
                partitionAssignmentHandler = this.partitionAssignmentHandler == null
                    ? default(Action<List<TopicPartition>>)
                    : partitions => this.partitionAssignmentHandler(consumer, partitions),
                offsetsCommittedHandler = this.offsetsCommittedHandler == null
                    ? default(Action<CommittedOffsets>)
                    : offsets => this.offsetsCommittedHandler(consumer, offsets)
            };
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.ConsumerBuilder(IEnumerable{KeyValuePair{string, string}})" />.
        /// </summary>
        public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.config = config;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetOffsetsCommittedHandler(Action{Consumer{TKey,TValue}, CommittedOffsets})" />.
        /// </summary>
        public ConsumerBuilder SetOffsetsCommittedHandler(
            Action<Consumer, CommittedOffsets> offsetsCommittedHandler)
        {
            this.offsetsCommittedHandler = offsetsCommittedHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetPartitionAssignmentRevokedHandler(Action{Consumer{TKey,TValue}, List{TopicPartition}})" />.
        /// </summary>
        public ConsumerBuilder SetPartitionsRevokedHandler(
            Action<Consumer, List<TopicPartition>> partitionsRevokedHandler)
        {
            this.partitionAssignmentRevokedHandler = partitionsRevokedHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetPartitionAssignmentHandler(Action{Consumer{TKey,TValue}, List{TopicPartition}})" />.
        /// </summary>
        public ConsumerBuilder SetPartitionsAssignedHandler(
            Action<Consumer, List<TopicPartition>> partitionsAssignedHandler)
        {
            this.partitionAssignmentHandler = partitionsAssignedHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetStatisticsHandler(Action{Consumer{TKey,TValue}, string})" />.
        /// </summary>
        public ConsumerBuilder SetStatisticsHandler(Action<Consumer, string> statisticsHandler)
        {
            this.statsHandler = statisticsHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetErrorHandler(Action{Consumer{TKey,TValue}, Error})" />.
        /// </summary>
        public ConsumerBuilder SetErrorHandler(Action<Consumer, Error> errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetLogHandler(Action{Consumer{TKey,TValue}, LogMessage})" />.
        /// </summary>
        public ConsumerBuilder SetLogHandler(Action<Consumer, LogMessage> logHandler)
        {
            this.logHandler = logHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.Build" />.
        /// </summary>
        public Consumer Build()
        {
            return new Consumer(this);
        }
    }


    /// <summary>
    ///     A builder class for <see cref="Consumer{TKey,TValue}" /> instances.
    /// </summary>
    public class ConsumerBuilder<TKey, TValue>
    {
        internal IEnumerable<KeyValuePair<string, string>> config;
        internal Action<Consumer<TKey, TValue>, Error> errorHandler;
        internal Action<Consumer<TKey, TValue>, LogMessage> logHandler;
        internal Action<Consumer<TKey, TValue>, string> statisticsHandler;
        internal Action<Consumer<TKey, TValue>, List<TopicPartition>> partitionAssignmentHandler;
        internal Action<Consumer<TKey, TValue>, List<TopicPartition>> partitionAssignmentRevokedHandler;
        internal Action<Consumer<TKey, TValue>, CommittedOffsets> offsetsCommittedHandler;

        internal IDeserializer<TKey> keyDeserializer;
        internal IDeserializer<TValue> valueDeserializer;
        internal IAsyncDeserializer<TKey> asyncKeyDeserializer;
        internal IAsyncDeserializer<TValue> asyncValueDeserializer;

        internal ConsumerBase.Config ConstructBaseConfig(Consumer<TKey, TValue> consumer)
        {
            return new ConsumerBase.Config
            {
                config = config,
                errorHandler = this.errorHandler == null
                    ? default(Action<Error>) // using default(...) rather than null (== default(...)) so types can be inferred.
                    : error => this.errorHandler(consumer, error),
                logHandler = this.logHandler == null
                    ? default(Action<LogMessage>)
                    : logMessage => this.logHandler(consumer, logMessage),
                statsHandler = this.statisticsHandler == null
                    ? default(Action<string>)
                    : stats => this.statisticsHandler(consumer, stats),
                partitionAssignmentRevokedHandler = this.partitionAssignmentRevokedHandler == null
                    ? default(Action<List<TopicPartition>>)
                    : partitions => this.partitionAssignmentRevokedHandler(consumer, partitions),
                partitionAssignmentHandler = this.partitionAssignmentHandler == null
                    ? default(Action<List<TopicPartition>>)
                    : partitions => this.partitionAssignmentHandler(consumer, partitions),
                offsetsCommittedHandler = this.offsetsCommittedHandler == null
                    ? default(Action<CommittedOffsets>)
                    : offsets => this.offsetsCommittedHandler(consumer, offsets)
            };
        }

        /// <summary>
        ///     Initialize a new ConsumerBuilder instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </param>
        public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.config = config;
        }

        /// <summary>
        ///     A handler that is called to report the result of (automatic) offset 
        ///     commits. It is not called as a result of the use of the Commit method.
        /// </summary>
        /// <remarks>
        ///     <paramref name="offsetsCommittedHandler" /> executes as a side-effect of
        ///     the Consumer.Consume call (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetOffsetsCommittedHandler(
            Action<Consumer<TKey, TValue>, CommittedOffsets> offsetsCommittedHandler)
        {
            this.offsetsCommittedHandler = offsetsCommittedHandler;
            return this;
        }

        /// <summary>
        ///     Set the partition assignment revoked handler.
        /// 
        ///     If you do not call the <see cref="Confluent.Kafka.ConsumerBase.Unassign" /> or 
        ///     <see cref="Confluent.Kafka.ConsumerBase.Assign(IEnumerable{TopicPartition})" />
        ///     (or other overload) method in your handler, all partitions will be  automatically
        ///     unassigned. This default behavior will not occur if you call Unassign (or Assign)
        ///     yourself.
        /// </summary>
        /// <remarks>
        ///     <paramref name="partitionAssignmentRevokedHandler" /> executes as a side-effect of
        ///     the Consumer.Consume call (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetPartitionAssignmentRevokedHandler(
            Action<Consumer<TKey,TValue>, List<TopicPartition>> partitionAssignmentRevokedHandler)
        {
            this.partitionAssignmentRevokedHandler = partitionAssignmentRevokedHandler;
            return this;
        }

        /// <summary>
        ///     Set the partition assignment handler.
        /// 
        ///     If you do not call the <see cref="Confluent.Kafka.ConsumerBase.Assign(IEnumerable{TopicPartition})" />
        ///     method (or another overload of this method) in this handler, or do not specify a partition assignment handler,
        ///     the consumer will be automatically assigned to the partition assignment set provided by the consumer group and
        ///     consumption will resume from the last committed offset for each partition, or if there is no committed offset,
        ///     in accordance with the `auto.offset.reset` configuration property. This default behavior will not occur if
        ///     you call Assign yourself in the handler. The set of partitions you assign to is not required to match the
        ///     assignment provided by the consumer group, but typically will.
        /// </summary>
        /// <remarks>
        ///     <paramref name="partitionAssignmentHandler" /> executes as a side-effect of
        ///     the Consumer.Consume call (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetPartitionAssignmentHandler(
            Action<Consumer<TKey, TValue>, List<TopicPartition>> partitionAssignmentHandler)
        {
            this.partitionAssignmentHandler = partitionAssignmentHandler;
            return this;
        }

        /// <summary>
        ///     Set the handler to call on librdkafka statistics events. Statistics are provided as a JSON formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     Executes as a side-effect of the Consume method (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetStatisticsHandler(
            Action<Consumer<TKey, TValue>, string> statisticsHandler)
        {
            this.statisticsHandler = statisticsHandler;
            return this;
        }

        /// <summary>
        ///     Set the handler to call on error events e.g. connection failures or all
        ///     brokers down. Note that the client will try to automatically recover from
        ///     errors that are not marked as fatal - such errors should be interpreted
        ///     as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consume method (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetErrorHandler(
            Action<Consumer<TKey, TValue>, Error> errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /// <summary>
        ///     Set the handler to call when there is information available
        ///     to be logged. If not specified, a default callback that writes
        ///     to stderr will be used.
        /// </summary>
        /// <remarks>
        ///     By default not many log messages are generated.
        ///
        ///     For more verbose logging, specify one or more debug contexts
        ///     using the 'debug' configuration property. The 'log_level'
        ///     configuration property is also relevant, however logging is
        ///     verbose by default given a debug context has been specified,
        ///     so you typically shouldn't adjust this value.
        ///
        ///     Warning: Log handlers are called spontaneously from internal
        ///     librdkafka threads and the application must not call any
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetLogHandler(
            Action<Consumer<TKey, TValue>, LogMessage> logHandler)
        {
            this.logHandler = logHandler;
            return this;
        }

        /// <summary>
        ///     Set the deserializer to use to deserialize keys.
        /// </summary>
        public ConsumerBuilder<TKey, TValue> SetKeyDeserializer(IDeserializer<TKey> deserializer)
        {
            this.keyDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Set the deserializer to use to deserialize values.
        /// </summary>
        public ConsumerBuilder<TKey, TValue> SetValueDeserializer(IDeserializer<TValue> deserializer)
        {
            this.valueDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Set the deserializer to use to deserialize keys.
        /// </summary>
        public ConsumerBuilder<TKey, TValue> SetKeyDeserializer(IAsyncDeserializer<TKey> deserializer)
        {
            this.asyncKeyDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Set the deserializer to use to deserialize values.
        /// </summary>
        public ConsumerBuilder<TKey, TValue> SetValueDeserializer(IAsyncDeserializer<TValue> deserializer)
        {
            this.asyncValueDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Build a new Consumer instance.
        /// </summary>
        public Consumer<TKey, TValue> Build()
        {
            return new Consumer<TKey, TValue>(this);
        }
    }
}
