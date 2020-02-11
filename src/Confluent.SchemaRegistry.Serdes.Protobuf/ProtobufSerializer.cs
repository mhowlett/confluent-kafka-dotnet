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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Google.Protobuf;
using Google.Protobuf.Reflection;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Protobuf Serializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the Protobuf schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class ProtobufSerializer<T> : IAsyncSerializer<T>  where T : IMessage<T>, new()
    {
        private bool autoRegisterSchema = true;
        private int initialBufferSize = DefaultInitialBufferSize;
        private ISchemaRegistryClient schemaRegistryClient;
        
        /// <summary>
        ///     The default initial size (in bytes) of buffers used for message 
        ///     serialization.
        /// </summary>
        public const int DefaultInitialBufferSize = 1024;

        private HashSet<string> subjectsRegistered = new HashSet<string>();

        private SemaphoreSlim serializeMutex = new SemaphoreSlim(1);

        private int? schemaId;

        /// <summary>
        ///     Initialize a new instance of the ProtobufSerializer class.
        /// </summary>
        public ProtobufSerializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;

            if (config == null) { return; }

            var nonProtobufConfig = config.Where(item => !item.Key.StartsWith("protobuf."));
            if (nonProtobufConfig.Count() > 0)
            {
                throw new ArgumentException($"ProtobufSerializer: unknown configuration parameter {nonProtobufConfig.First().Key}");
            }

            var protobufConfig = config.Where(item => item.Key.StartsWith("protobuf."));
            if (protobufConfig.Count() == 0) { return; }

            int? initialBufferSize = (int?)Utils.ExtractPropertyValue(config, ProtobufSerializerConfig.PropertyNames.BufferBytes, "ProtobufSerializer", typeof(int));
            if (initialBufferSize != null) { this.initialBufferSize = initialBufferSize.Value; }

            bool? autoRegisterSchema = (bool?)Utils.ExtractPropertyValue(config, ProtobufSerializerConfig.PropertyNames.AutoRegisterSchemas, "ProtobufSerializer", typeof(bool));
            if (autoRegisterSchema != null) { this.autoRegisterSchema = autoRegisterSchema.Value; }

            foreach (var property in protobufConfig)
            {
                if (property.Key != ProtobufSerializerConfig.PropertyNames.AutoRegisterSchemas && property.Key != ProtobufSerializerConfig.PropertyNames.BufferBytes)
                {
                    throw new ArgumentException($"ProtobufSerializer: unknown configuration parameter {property.Key}");
                }
            }
        }

        private void writeMessageIndices(MessageDescriptor md, Stream output)
        {
            Span<byte> result = stackalloc byte[Utils.MAX_SINGLE_BYTE_VARINT];
            int resultLoc = 0;

            // Walk the nested MessageDescriptor tree up to the root.
            var currentMd = md;
            while (currentMd.ContainingType != null)
            {
                var prevMd = currentMd;
                currentMd = currentMd.ContainingType;
                for (int i=0; i<currentMd.NestedTypes.Count; ++i)
                {
                    if (currentMd.NestedTypes[i].ClrType == prevMd.ClrType)
                    {
                        if (i > Utils.MAX_SINGLE_BYTE_VARINT)
                        {
                            throw new NotImplementedException($"Nested descriptor index {i} exceeded maximum supported: {Utils.MAX_SINGLE_BYTE_VARINT}");
                        }
                        result[resultLoc++] = (byte)i;
                        if (resultLoc > Utils.MAX_SINGLE_BYTE_VARINT)
                        {
                            throw new NotImplementedException($"Max nesting of {Utils.MAX_SINGLE_BYTE_VARINT} exceeded");
                        }
                        break;
                    }
                }
            }

            // Add the index of the root MessageDescriptor in the FileDescriptor.
            for (int i=0; i<md.File.MessageTypes.Count; ++i)
            {
                if (md.File.MessageTypes[i].ClrType == currentMd.ClrType)
                {
                    if (i > Utils.MAX_SINGLE_BYTE_VARINT)
                    {
                        throw new NotImplementedException($"Nested descriptor index {i} exceeded maximum supported: {Utils.MAX_SINGLE_BYTE_VARINT}");
                    }
                    result[resultLoc++] = (byte)i;
                    if (resultLoc > Utils.MAX_SINGLE_BYTE_VARINT)
                    {
                        throw new NotImplementedException($"Max nesting of {Utils.MAX_SINGLE_BYTE_VARINT} exceeded");
                    }
                    break;
                }
            }

            if (resultLoc == 0)
            {
                throw new ArgumentException("MessageDescriptor not found");
            }

            output.WriteByte((byte)resultLoc);
            for (int i=0; i<resultLoc; ++i)
            {
                output.WriteByte(result[resultLoc-i-1]);
            }
        }

        /// <summary>
        ///     Serialize an instance of type <typeparamref name="T"/> to a byte array in Protobuf format. The serialized
        ///     data is preceded by a "magic byte" (1 byte) and the id of the schema as registered
        ///     in Confluent's Schema Registry (4 bytes, network byte order).
        ///     todo: then inner.
        ///     This call may block or throw 
        ///     on first use for a particular topic during schema registration.
        /// </summary>
        /// <param name="value">
        ///     The value to serialize.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the serialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes with 
        ///     <paramref name="value" /> serialized as a byte array.
        /// </returns>
        public async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            try
            {
                await serializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    string fullname = value.Descriptor.FullName;

                    string subject = context.Component == MessageComponentType.Key
                        ? schemaRegistryClient.ConstructKeySubjectName(context.Topic, fullname)
                        : schemaRegistryClient.ConstructValueSubjectName(context.Topic, fullname);

                    if (!subjectsRegistered.Contains(subject))
                    {
                        // first usage: register/get schema to check compatibility
                        schemaId = autoRegisterSchema
                            ? await schemaRegistryClient.RegisterSchemaAsync(subject, value.Descriptor.File.SerializedData.ToBase64()).ConfigureAwait(continueOnCapturedContext: false)
                            : await schemaRegistryClient.GetSchemaIdAsync(subject, value.Descriptor.File.SerializedData.ToBase64()).ConfigureAwait(continueOnCapturedContext: false);

                        subjectsRegistered.Add(subject);
                    }
                }
                finally
                {
                    serializeMutex.Release();
                }

                using (var stream = new MemoryStream(initialBufferSize))
                using (var writer = new BinaryWriter(stream))
                {
                    stream.WriteByte(Constants.MagicByte);
                    writer.Write(IPAddress.HostToNetworkOrder(schemaId.Value));
                    writeMessageIndices(value.Descriptor, stream);
                    value.WriteTo(stream);
                    return stream.ToArray();
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}
