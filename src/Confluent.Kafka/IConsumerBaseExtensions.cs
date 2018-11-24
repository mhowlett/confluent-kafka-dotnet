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


namespace Confluent.Kafka
{
    public static class IConsumerBaseExtensions
    {
        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="consumer"></param>
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
        public static ConsumeResult Consume(this IConsumerBase consumer, CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var result = consumer.Consume(100, Deserializers.ByteArray, Deserializers.ByteArray);
                if (result == null) { continue; }
                return new ConsumeResult
                {
                    TopicPartitionOffset = result.TopicPartitionOffset,
                    Message = new Message
                    {
                        Timestamp = result.Timestamp,
                        Headers = result.Headers,
                        Key = result.Key,
                        Value = result.Value
                    }
                };
            }
        }


        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the timeout period has elapsed.
        /// </summary>
        /// <param name="consumer"></param>
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
        public static ConsumeResult Consume(this IConsumerBase consumer, TimeSpan timeout)
            => consumer.Consume(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the timeout period has elapsed.
        /// </summary>
        /// <param name="consumer"></param>
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
        public static ConsumeResult Consume(this IConsumerBase consumer, int millisecondsTimeout)
        {
            var result = consumer.Consume(millisecondsTimeout, Deserializers.ByteArray, Deserializers.ByteArray);
            if (result == null) { return null; }
            return new ConsumeResult
            {
                TopicPartitionOffset = result.TopicPartitionOffset,
                Message = new Message
                {
                    Timestamp = result.Timestamp,
                    Headers = result.Headers,
                    Key = result.Key,
                    Value = result.Value
                }
            };
        }
    }
}
