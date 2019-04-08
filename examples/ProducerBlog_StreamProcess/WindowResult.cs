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

        public static SimpleSerializer<WindowResult<TKey, TValue>> CreateSerializer(SimpleSerializer<TKey> keySerializer, SimpleSerializer<TValue> valueSerializer) =>
            data =>
            {
                var bs1 = SimpleSerializers.Int64(data.WindowStart.UnixTimestampMs);
                var bs3 = keySerializer(data.Key);
                var bs2 = SimpleSerializers.Int32(bs3.Length);
                var bs4 = valueSerializer(data.Value);
                var result = new byte[bs1.Length + bs2.Length + bs3.Length + bs4.Length];
                bs1.CopyTo(result, 0);
                bs2.CopyTo(result, bs1.Length);
                bs3.CopyTo(result, bs1.Length + bs2.Length);
                bs4.CopyTo(result, bs1.Length + bs2.Length + bs3.Length);
                return result;
            };

        public static Deserializer<WindowResult<TKey, TValue>> CreateDeserializer(Deserializer<TKey> keyDeserializer, Deserializer<TValue> valueDeserializer) =>
            (data, isNull) => 
            {
                var timestamp = Deserializers.Int64(data.Slice(0, sizeof(long)), false);
                var keyLen = Deserializers.Int32(data.Slice(sizeof(long), sizeof(int)), false);
                var key = keyDeserializer(data.Slice(sizeof(long) + sizeof(int)), false);
                var val = valueDeserializer(data.Slice(sizeof(long) + 2*sizeof(int) + keyLen), false);
                return new WindowResult<TKey, TValue>(new Timestamp(timestamp, TimestampType.NotAvailable), key, val);
            };

    }
}
