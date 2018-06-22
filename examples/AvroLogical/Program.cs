using System;
using System.Collections.Generic;
using System.IO;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Avro;
using Avro.Specific;

/*
{ 
  "namespace": "MessageTypes",
  "type": "record",
  "doc": "Demonstrates use of logical types.",
  "name": "LogicalTypeExample",
  "fields": [
    { "name": "StringValue", "type": "string" },
    { "name": "DateValue", "type": { "type": "int", "logicalType": "date" } },
    { "name": "DecimalValue", "type": { "type": "bytes", "logicalType": "decimal", "precision": "8", "scale": "2" } }
  ]
}
*/

namespace AvroLogical
{
    public class EnhancedExample : MessageTypes.LogicalTypeExample
    {
        public static Schema _SCHEMA = MessageTypes.LogicalTypeExample._SCHEMA;
        private const int DECIMAL_SCALE = 2;
        private static readonly IntSerializer serializer = new IntSerializer();
        private static readonly IntDeserializer deserializer = new IntDeserializer();

        public EnhancedExample() {}

        public EnhancedExample(MessageTypes.LogicalTypeExample other)
        {
            this.DateValue = other.DateValue;
            this.DecimalValue = other.DecimalValue;
            this.StringValue = other.StringValue;
        }

        public DateTime DateValue_AsDateTime
        {
            get
            {
                return Timestamp.UnixTimestampMsToDateTime(this.DateValue*1000);
            }
            set
            {
                this.DateValue = (int)(Timestamp.DateTimeToUnixTimestampMs(value)/1000);
            }
        }

        // TODO: this encoding / decoding is limited. not finished. proof of concept only.
        public Decimal DecimalValue_AsDecimal
        {
            get
            {
                var bits = new int[4];
                bits[0] = deserializer.Deserialize(null, DecimalValue);
                bits[1] = 0;
                bits[2] = 0;
                bits[3] = DECIMAL_SCALE << 16;
                return new Decimal(bits);
             }
            set
            {
                // https://msdn.microsoft.com/en-us/library/system.decimal.getbits.aspx
                var bits = Decimal.GetBits(value);
                if (bits[1] != 0) { throw new ArgumentException("decimal value out of range for this serializer"); }
                if (bits[2] != 0) { throw new ArgumentException("decimal value out of range for this serializer"); }
                
                bool isNegative = bits[3] < 0; // bit 31 determines sign in two's complement.
                int zero1 = bits[3] & 0b0000_0000_0000_0000_1111_1111_1111_1111;
                int zero2 = bits[3] & 0b0111_1111_0000_0000_0000_0000_0000_0000;
                int exp =  (bits[3] & 0b0000_0000_1111_1111_0000_0000_0000_0000) >> 16;

                if (isNegative) { throw new ArgumentException("negative decimals not supported by this serializer"); }
                if (zero1 != 0) { throw new ArgumentException("Bits 0 to 15, the lower word, are unused and must be zero."); }
                if (zero2 != 0) { throw new ArgumentException("Bits 24 to 30 are unused and must be zero."); }
                if (exp != DECIMAL_SCALE) { throw new ArgumentException($"decimal scale does not match schema (must be {DECIMAL_SCALE})"); }

                DecimalValue = serializer.Serialize(null, bits[0]);
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
           var pConfig = new Dictionary<string, object> { { "bootstrap.servers", "10.200.7.144:9092" }, { "schema.registry.url", "localhost:8081" } };
            using (var p = new Producer<Null, EnhancedExample>(pConfig, null, new AvroSerializer<EnhancedExample>()))
            {
                var dr = p.ProduceAsync("avrotest", new Message<Null, EnhancedExample> { Value = new EnhancedExample { StringValue = "test-string", DecimalValue_AsDecimal = 120.95M, DateValue_AsDateTime = DateTime.UtcNow } }).Result;
            }

            var cConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", "10.200.7.144:9092" },
                { "schema.registry.url", "localhost:8081" }, 
                { "group.id", "testgroup" },
                { "auto.offset.reset", "beginning" }
            };
            using (var c = new Consumer<Null, MessageTypes.LogicalTypeExample>(cConfig, null, new AvroDeserializer<MessageTypes.LogicalTypeExample>()))
            {
                c.OnConsumeError += (_, e) => Console.WriteLine(e.Error.Reason);
                c.OnPartitionEOF += (_, p) => Console.WriteLine(p.TopicPartition);
                c.Assign(new TopicPartition("avrotest", 0));
                ConsumerRecord<Null, MessageTypes.LogicalTypeExample> record;
                while (!c.Consume(out record, 10000));
                EnhancedExample ee = new EnhancedExample(record.Value);
                Console.WriteLine($"decimal: {ee.DecimalValue_AsDecimal}  date: {ee.DateValue_AsDateTime}");
            }

            var srConfig = new Dictionary<string, object> { { "schema.registry.url", "localhost:8081" } };
            var sr = new Confluent.SchemaRegistry.CachedSchemaRegistryClient(srConfig);
            var srResult = sr.RegisterSchemaAsync("avrotest-value", File.ReadAllText("/git/confluent-kafka-dotnet/examples/AvroLogical/LogicalTypeExample.asvc")).Result;
        }
    }
}
