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
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka consumer.
    /// </summary>
    public interface IConsumerAsync : IConsumerBase
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer.Consume(CancellationToken)" />
        /// </summary>
        Task<ConsumeResult> ConsumeAsync(CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer.Consume(TimeSpan)" />
        /// </summary>
        Task<ConsumeResult> ConsumeAsync(TimeSpan timeout);

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
        Task<ConsumeResult> ConsumeAsync(int millisecondsTimeout);
    }

    /// <summary>
    ///     Defines a high-level Apache Kafka consumer (with key and 
    ///     value deserialization).
    /// </summary>
    public interface IConsumerAsync<TKey, TValue> : IConsumerBase
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        /// </summary>
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(TimeSpan)" />
        /// </summary>
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(TimeSpan timeout);
        
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
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(int millisecondsTimeout);
    }
}
