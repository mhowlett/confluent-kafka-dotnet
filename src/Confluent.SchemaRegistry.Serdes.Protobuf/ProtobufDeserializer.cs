
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using Confluent.Kafka;
using Google.Protobuf;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) Protobuf deserializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the Avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class ProtobufDeserializer<T> : IAsyncDeserializer<T> where T : IMessage<T>, new()
    {
        private MessageParser<T> parser;


        /// <summary>
        ///     Initialize a new ProtobufDeserializer instance.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     An implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to 
        ///     <see cref="ProtobufDeserializerConfig" />).
        /// </param>
        public ProtobufDeserializer(IEnumerable<KeyValuePair<string, string>> config = null)
        {
            this.parser = new MessageParser<T>(() => new T());

            if (config == null) { return; }

            var nonProtobufConfig = config.Where(item => !item.Key.StartsWith("protobuf."));
            if (nonProtobufConfig.Count() > 0)
            {
                throw new ArgumentException($"ProtobufDeserializer: unknown configuration parameter {nonProtobufConfig.First().Key}.");
            }

            var protobufConfig = config.Where(item => item.Key.StartsWith("protobuf."));
            if (protobufConfig.Count() != 0)
            {
                throw new ArgumentException($"ProtobufDeserializer: unknown configuration parameter {protobufConfig.First().Key}");
            }
        }

        public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            try
            {
                using (var stream = new MemoryStream(data.ToArray()))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != Constants.MagicByte)
                    {
                        // may change in the future.
                        throw new InvalidDataException($"Magic byte should be {Constants.MagicByte}, not {magicByte}");
                    }
                    var _ = IPAddress.NetworkToHostOrder(reader.ReadInt32()); // un-needed.

                    var indicesLength = reader.ReadByte();
                    if (indicesLength > Utils.MAX_SINGLE_BYTE_VARINT)
                    {
                        throw new NotImplementedException($"Maximum message descriptor index exceeded: {Utils.MAX_SINGLE_BYTE_VARINT}");
                    }
                    stream.Seek(indicesLength, SeekOrigin.Current);
                    return Task.FromResult(parser.ParseFrom(stream));
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}
