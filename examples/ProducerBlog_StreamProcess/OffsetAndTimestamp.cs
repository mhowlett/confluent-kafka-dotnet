using System;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public class OffsetAndTimestamp
    {
        public OffsetAndTimestamp(long offset, Timestamp window)
        {
            Window = window;
            Offset = offset;
        }

        public long Offset { get; private set; }

        public Timestamp Window { get; private set; }

        public static SimpleSerializer<OffsetAndTimestamp> GetSerializer() =>
            data => 
            {
                var bs1 = SimpleSerializers.Int64(data.Offset);
                var bs2 = SimpleSerializers.Int64(data.Window.UnixTimestampMs);
                var result = new byte[bs1.Length + bs2.Length];
                bs1.CopyTo(result, 0);
                bs1.CopyTo(result, bs1.Length);
                return result;
            };

        public static Deserializer<OffsetAndTimestamp> GetDeserializer() =>
            (data, isNull) => 
                new OffsetAndTimestamp(
                    Deserializers.Int64(data.Slice(0, sizeof(long)), false), 
                    new Timestamp(Deserializers.Int64(data.Slice(sizeof(long), sizeof(long)), false), TimestampType.NotAvailable));
    }
}
