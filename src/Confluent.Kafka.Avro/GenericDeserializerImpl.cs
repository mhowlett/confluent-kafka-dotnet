﻿// Copyright 2018 Confluent Inc.
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

using System.Collections.Concurrent;
using System.IO;
using System.Net;
using Avro.IO;
using Avro.Generic;
using Confluent.SchemaRegistry;


namespace Confluent.Kafka.Serialization
{
    internal class GenericDeserializerImpl : IAvroDeserializerImpl<GenericRecord>
    {
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen)
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly ConcurrentDictionary<int, DatumReader<GenericRecord>> datumReaderBySchemaId 
            = new ConcurrentDictionary<int, DatumReader<GenericRecord>>();

        private ISchemaRegistryClient schemaRegistryClient;

        public GenericDeserializerImpl(ISchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = schemaRegistryClient;
        }

        public GenericRecord Deserialize(string topic, byte[] array)
        {
            // Note: topic is not necessary for deserialization (or knowing if it's a key 
            // or value) only the schema id is needed.

            using (var stream = new MemoryStream(array))
            using (var reader = new BinaryReader(stream))
            {
                var magicByte = reader.ReadByte();
                if (magicByte != Constants.MagicByte)
                {
                    // may change in the future.
                    throw new InvalidDataException($"magic byte should be 0, not {magicByte}");
                }
                var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                // Note: there is a race condition here in which more than one datumReader can
                // be created for a given writerId, but this is benign (doesn't matter) and 
                // happens very infrequently => doesn't affect the effectiveness of the cache.

                datumReaderBySchemaId.TryGetValue(writerId, out DatumReader<GenericRecord> datumReader);
                if (datumReader == null)
                {
                    // TODO: If any of this cache fills up, this is probably an
                    // indication of misuse of the deserializer. Ideally we would do 
                    // something more sophisticated than the below + not allow 
                    // the misuse to keep happening without warning.
                    if (datumReaderBySchemaId.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        datumReaderBySchemaId.Clear();
                    }

                    var writerSchemaJson = schemaRegistryClient.GetSchemaAsync(writerId).Result;
                    var writerSchema = Avro.Schema.Parse(writerSchemaJson);

                    datumReader = new GenericReader<GenericRecord>(writerSchema, writerSchema);
                    datumReaderBySchemaId[writerId] = datumReader;
                }

                return datumReader.Read(default(GenericRecord), new BinaryDecoder(stream));
            }
        }

    }
}
