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
using System.Threading.Tasks;
using Confluent.Kafka;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Protobuf Serializer
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the Protobuf schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class ProtobufSerializer<T> : IAsyncSerializer<T>  where T : IMessage<T>, new()
    {
        public ProtobufSerializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null)
        {
            
        }

        private static string FieldTypeToString(FieldType ft)
            => ft.ToString().ToLower();


        public Task<byte[]> SerializeAsync(T data, SerializationContext context)
        {
            // var d = data.Descriptor;
            return Task.FromResult(data.ToByteArray());
        }
    }
}


/*

Notes:

In serializer, need to go from Descriptor -> .proto file format which gets sent to SR.
In java, this is done via square.wire.
I don't think there's an equivalant in C#, but it shouldn't be too hard.
Square.wire also allows you to go the other way (and get dynamic message type). This is harder.
This will remain out of scope in .net.
-- 
What is sent to schema registry was previously just the schema. 
Now it's something with format:

schema: " .... "
schemaType: AVRO, PROTOBUF,
references: { name: "foo.proto",
              subject: "mysubj",
              version: 3 }

However, the new serializer will still send the old format in the case of AVRO, even
when new SR is used, for compatibility.

*/