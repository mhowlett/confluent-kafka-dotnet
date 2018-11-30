// Copyright 2016-2018 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer with
    ///     deserializater capability.
    /// </summary>
    public class ConsumerAsync<TKey, TValue> : ConsumerBase, IConsumerAsync<TKey, TValue>
    {
        private IAsyncDeserializer<TKey> keyDeserializer;
        private IAsyncDeserializer<TValue> valueDeserializer;

        /// <summary>
        ///     Creates a new <see cref="Confluent.Kafka.Consumer{TKey,TValue}" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </param>
        /// <param name="keyDeserializer">
        ///     The deserializer to use to deserialize keys.
        /// </param>
        /// <param name="valueDeserializer">
        ///     The deserializer to use to deserialize values.
        /// </param>
        public ConsumerAsync(
            IEnumerable<KeyValuePair<string, string>> config,
            IAsyncDeserializer<TKey> keyDeserializer,
            IAsyncDeserializer<TValue> valueDeserializer
        ) : base(config)
        {
            this.keyDeserializer = keyDeserializer ?? throw new ArgumentNullException(nameof(keyDeserializer));
            this.valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));
        }


        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="millisecondsTimeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked, OnOffsetsCommitted and
        ///     OnPartitionEOF events may be invoked as a side-effect of
        ///     calling this method (on the same thread).
        /// </remarks>
        public async Task<ConsumeResult<TKey, TValue>> ConsumeAsync(int millisecondsTimeout)
        {
            // TODO: change the Consume method, or add to ConsumerBase to expose raw data, and push
            // burden of msgPtr dispose on the caller.
            var rawResult = Consume(millisecondsTimeout, Deserializers.ByteArray, Deserializers.ByteArray);
            if (rawResult == null) { return null; }

            TKey key = await keyDeserializer.DeserializeAsync(rawResult.Key, rawResult.Key == null, true, rawResult.Message, rawResult.TopicPartition);
            TValue val = await valueDeserializer.DeserializeAsync(rawResult.Value, rawResult.Value == null, false, rawResult.Message, rawResult.TopicPartition);

            return new ConsumeResult<TKey, TValue>
            {
                TopicPartitionOffset = rawResult.TopicPartitionOffset,
                Message = new Message<TKey, TValue>
                {
                    Key = key,
                    Value = val,
                    Headers = rawResult.Headers,
                    Timestamp = rawResult.Timestamp
                }
            };
        }

        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked, OnOffsetsCommitted and
        ///     OnPartitionEOF events may be invoked as a side-effect of
        ///     calling this method (on the same thread).
        /// </remarks>
        public async Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await ConsumeAsync(100);

                if (result != null)
                {
                    return result;
                }
            }

            return null;
        }

        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the timeout period has elapsed.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked, OnOffsetsCommitted and 
        ///     OnPartitionEOF events may be invoked as a side-effect of 
        ///     calling this method (on the same thread).
        /// </remarks>
        public async Task<ConsumeResult<TKey, TValue>> ConsumeAsync(TimeSpan timeout) => await ConsumeAsync(timeout.TotalMillisecondsAsInt());
    }
}
