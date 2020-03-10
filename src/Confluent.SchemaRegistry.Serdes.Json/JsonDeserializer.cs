// Copyright 2020 Confluent Inc.
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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) Json deserializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           A magic byte that identifies this as a message with
    ///                         Confluent Platform framing.
    ///       bytes 1-4:        Unique global id of the JSON schema associated with
    ///                         the data (as registered in Confluent Schema Registry),
    ///                         big endian.
    ///       following bytes:  The JSON data (utf8)
    ///
    ///     Internally, Newtonsoft.Json for deserialization. Currently,
    ///     no explicity validation of the data is done against the
    ///     schema stored in Schema Registry.
    ///
    ///     Note: Off-the-shelf libraries do not yet exist to enable
    ///     integration of System.Text.Json and JSON Schema, so this
    ///     is not yet supported by the deserializer.
    /// </remarks>
    public class JsonDeserializer<T> : IAsyncDeserializer<T> where T : class, new()
    {
        private readonly int headerSize =  sizeof(int) + 1;

        /// <summary>
        ///     Initialize a new JsonDeserializer instance.
        /// </summary>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to
        ///     <see cref="JsonDeserializerConfig" />).
        /// </param>
        public JsonDeserializer(IEnumerable<KeyValuePair<string, string>> config = null)
        {
            if (config == null) { return; }

            if (config.Count() > 0)
            {
                throw new ArgumentException($"JsonDeserializer: unknown configuration parameter {config.First().Key}.");
            }
        }

        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/>
        ///     from a byte array.
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the deserialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) { return Task.FromResult<T>(null); }

            try
            {
                var array = data.ToArray();
                if (array[0] != Constants.MagicByte)
                {
                    throw new InvalidDataException($"Expecting magic byte to be {Constants.MagicByte}, not {array[0]}");
                }

                // A schema is not required to deserialize json messages.
                // TODO: add validation capability.

                using (var stream = new MemoryStream(array, headerSize, array.Length - headerSize))
                using (var sr = new System.IO.StreamReader(stream, Encoding.UTF8))
                {
                    return Task.FromResult(Newtonsoft.Json.JsonConvert.DeserializeObject<T>(sr.ReadToEnd()));
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}