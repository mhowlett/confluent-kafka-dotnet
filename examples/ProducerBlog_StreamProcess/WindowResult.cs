using System;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public class WindowResult<TKey, TValue>
    {
        public WindowResult(Timestamp windowStart, TKey key, TValue value)
        {
            WindowStart = windowStart;
            Key = key;
            Value = value;
        }

        public Timestamp WindowStart { get; private set; }

        public TKey Key { get; private set; }

        public TValue Value { get; private set; }

        public static ISerializer<WindowResult<TKey, TValue>> CreateSerializer(
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer)
                => new Serializer(keySerializer, valueSerializer);

        public static IDeserializer<WindowResult<TKey, TValue>> CreateDeserializer(
            IDeserializer<TKey> keyDeserializer,
            IDeserializer<TValue> valueDeserializer)
                => new Deserializer(keyDeserializer, valueDeserializer);

        public class Serializer : ISerializer<WindowResult<TKey, TValue>>
        {
            ISerializer<TKey> keySerializer;
            ISerializer<TValue> valueSerializer;

            public Serializer(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
            {
                this.keySerializer = keySerializer;
                this.valueSerializer = valueSerializer;
            }

            public byte[] Serialize(WindowResult<TKey, TValue> data, SerializationContext context)
            {
                var bs1 = Serializers.Int64.Serialize(data.WindowStart.UnixTimestampMs, SerializationContext.Empty);
                var bs3 = keySerializer.Serialize(data.Key, SerializationContext.Empty);
                var bs2 = Serializers.Int32.Serialize(bs3.Length, SerializationContext.Empty);
                var bs4 = valueSerializer.Serialize(data.Value, SerializationContext.Empty);
                var result = new byte[bs1.Length + bs2.Length + bs3.Length + bs4.Length];
                bs1.CopyTo(result, 0);
                bs2.CopyTo(result, bs1.Length);
                bs3.CopyTo(result, bs1.Length + bs2.Length);
                bs4.CopyTo(result, bs1.Length + bs2.Length + bs3.Length);
                return result;
            }
        }

        public class Deserializer : IDeserializer<WindowResult<TKey, TValue>>
        {
            IDeserializer<TKey> keyDeserializer;
            IDeserializer<TValue> valueDeserializer;

            public Deserializer(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
            {
                this.keyDeserializer = keyDeserializer;
                this.valueDeserializer = valueDeserializer;
            }

            public WindowResult<TKey, TValue> Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                var timestamp = Deserializers.Int64.Deserialize(data.Slice(0, sizeof(long)), false, SerializationContext.Empty);
                var keyLen = Deserializers.Int32.Deserialize(data.Slice(sizeof(long), sizeof(int)), false, SerializationContext.Empty);
                var key = keyDeserializer.Deserialize(data.Slice(sizeof(long) + sizeof(int)), false, SerializationContext.Empty);
                var val = valueDeserializer.Deserialize(data.Slice(sizeof(long) + 2*sizeof(int) + keyLen), false, SerializationContext.Empty);
                return new WindowResult<TKey, TValue>(new Timestamp(timestamp, TimestampType.NotAvailable), key, val);
            }
        }

    }
}
