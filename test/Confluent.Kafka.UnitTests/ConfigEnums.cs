using Xunit;

namespace Confluent.Kafka.UnitTests
{
    public class ConfigEnumTests
    {
        [Fact]
        public void ConsumerEnumProperties()
        {
            var config = new ConsumerConfig
            {
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            var value = config.AutoOffsetReset;

            Assert.Equal(AutoOffsetResetType.Earliest, value);
        }

        [Fact]
        public void ProducerEnumProperties()
        {
            var config = new ProducerConfig
            {
                Partitioner = PartitionerType.Consistent
            };

            Assert.Equal(PartitionerType.Consistent, config.Partitioner);
        }
    }
}
