
using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) Avro deserializer. Use this deserializer with GenericRecord,
    ///     types generated using the avrogen.exe tool or one of the following 
    ///     primitive types: int, long, float, double, boolean, string, byte[].
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the Avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class ProtobufDeserializer<T> : IAsyncDeserializer<T>
    {
        public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
