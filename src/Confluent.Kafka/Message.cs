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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a (deserialized) Kafka message.
    /// </summary>
    public class Message<TKey, TValue> : MessageMetadata
    {
        /// <summary>
        ///     Gets the message key value (possibly null).
        /// </summary>
        public TKey Key { get; set; }

        /// <summary>
        ///     Gets the message value (possibly null).
        /// </summary>
        public TValue Value { get; set; }
    }

    /// <summary>
    ///     Represents a Kafka message.
    /// </summary>
    public class Message : MessageMetadata
    {
        /// <summary>
        ///     Create a new Message instance with default values.
        /// </summary>
        public Message() {}

        /// <summary>
        ///     Create a new Message instance that is a copy of
        ///     <paramref name="message" />.
        /// </summary>
        /// <param name="message">
        ///     The <see cref="Message{TKey,TValue}" /> instance
        ///     to create a copy of.
        /// </param>
        public Message(Message<byte[], byte[]> message)
            : base(message)
        {
            Key = message.Key;
            Value = message.Value;
        }

        /// <summary>
        ///     Gets the message key value (possibly null).
        /// </summary>
        public byte[] Key { get; set; }

        /// <summary>
        ///     Gets the message value (possibly null).
        /// </summary>
        public byte[] Value { get; set; }
    }
}
