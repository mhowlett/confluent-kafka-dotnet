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

        public long Offset { get; set; }

        public Timestamp Window { get; set; }

        public class Serializer : ISerializer<OffsetAndTimestamp>
        {
            public byte[] Serialize(OffsetAndTimestamp data, SerializationContext context)
            {
                throw new System.NotImplementedException();
            }
        }

        public class Deserializer : IDeserializer<OffsetAndTimestamp>
        {
            public OffsetAndTimestamp Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                throw new NotImplementedException();
            }
        }
    }
}
