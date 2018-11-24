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

namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer.
    /// </summary>
    public class Consumer : ConsumerBase, IConsumer
    {
        private readonly Dictionary<Type, object> deserializers = new Dictionary<Type, object>();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="config"></param>
        public Consumer(IEnumerable<KeyValuePair<string, string>> config) : base(config)
        {
            deserializers.Add(typeof(string), Deserializers.UTF8);
            deserializers.Add(typeof(int), Deserializers.Int32);
            deserializers.Add(typeof(long), Deserializers.Long);
            deserializers.Add(typeof(float), Deserializers.Float);
            deserializers.Add(typeof(double), Deserializers.Double);
            deserializers.Add(typeof(Null), Deserializers.Null);
            deserializers.Add(typeof(Ignore), Deserializers.Ignore);
            deserializers.Add(typeof(byte[]), Deserializers.ByteArray);
        }

        /// <summary>
        ///     Sets the deserializer that will be used to deserialize keys or values with
        ///     the specified type.
        /// </summary>
        /// <param name="deserializer">
        ///     The deserializer.
        /// </param>
        public void RegisterDeserializer<T>(Deserializer<T> deserializer)
        {
            deserializers[typeof(T)] = deserializer;
        }

        /// <summary>
        ///     Removes the deserializer associated with the specified type.
        /// </summary>
        public void UnregisterDeserilizer<T>()
        {
            deserializers.Remove(typeof(T));
        }

        /// <summary>
        ///     Gets the deserializer that will be used to deserialize values of the specified type.
        /// </summary>
        /// <returns>
        ///     The deserializer corresponding to the specified type.
        /// </returns>
        public Deserializer<T> GetDeserializer<T>()
        {
            if (deserializers.TryGetValue(typeof(T), out var d))
                return (Deserializer<T>)d;
            throw new ArgumentException($"No deserializer associated with type ${typeof(T).Name}");
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
        public ConsumeResult<TKey, TValue> Consume<TKey, TValue>(CancellationToken cancellationToken = default(CancellationToken))
        {
            var keyDeserializer = GetDeserializer<TKey>();
            var valueDeserializer = GetDeserializer<TValue>();

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var result = Consume(100, keyDeserializer, valueDeserializer);
                if (result != null)
                    return result;
            }
        }

        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the timeout period has elapsed.
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
        public ConsumeResult<TKey, TValue> Consume<TKey, TValue>(int millisecondsTimeout)
            => Consume(millisecondsTimeout, GetDeserializer<TKey>(), GetDeserializer<TValue>());

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
        public ConsumeResult<TKey, TValue> Consume<TKey, TValue>(TimeSpan timeout)
            => Consume<TKey, TValue>(timeout.TotalMillisecondsAsInt());
    }

    /// <summary>
    ///     Implements a high-level Apache Kafka consumer.
    /// </summary>
    public class Consumer<TKey, TValue> : ConsumerBase, IConsumer<TKey, TValue>
    {
        private readonly Deserializer<TKey> keyDeserializer;
        private readonly Deserializer<TValue> valueDeserializer;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="config"></param>
        /// <param name="keyDeserializer"></param>
        /// <param name="valueDeserializer"></param>
        public Consumer(
            IEnumerable<KeyValuePair<string, string>> config,
            Deserializer<TKey> keyDeserializer = null,
            Deserializer<TValue> valueDeserializer = null
            ) : base(config)
        {
            this.keyDeserializer = keyDeserializer ?? Deserializers.GetBuiltin<TKey>();
            this.valueDeserializer = valueDeserializer ?? Deserializers.GetBuiltin<TValue>();
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
        public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var result = Consume(100, keyDeserializer, valueDeserializer);
                if (result != null)
                    return result;
            }
        }

        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the timeout period has elapsed.
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
        public ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
            => Consume(millisecondsTimeout, keyDeserializer, valueDeserializer);

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
        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
            => Consume(timeout.TotalMillisecondsAsInt());
    }
}
