using System;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public class TimeWindowCommitManager : IDisposable
    {
        string commitTopic;
        IProducer<Null, WindowCommit> commitProducer;
        IConsumer<Null, WindowCommit> commitConsumer;

        public class WindowCommit
        {
            public WindowCommit(int partition, long offset, long windowId)
            {
                Partition = partition;
                Offset = offset;
                WindowId = windowId;
            }

            public int Partition { get; set; }
            public long Offset { get; set; }

            /// <summary>
            ///     The window this corresponds to this consumer offset.
            /// </summary>
            public long WindowId { get; set; }

            public class Serializer : ISerializer<WindowCommit>
            {
                public byte[] Serialize(WindowCommit data, SerializationContext context)
                {
                    throw new NotImplementedException();
                }
            }
        }

        public class OffsetWindowId
        {
            public OffsetWindowId(long offset, long windowId)
            {

            }
        }

        public TimeWindowCommitManager(string brokerAddress, string commitTopic)
        {
            this.commitTopic = commitTopic;

            var pConfig = new ProducerConfig
            {
                ClientId = "window-commit-manager-producer",
                BootstrapServers = brokerAddress
            };

            var cConfig = new ConsumerConfig
            {
                ClientId = "window-commit-manager-consumer",
                GroupId = "window-commit-manager",
                BootstrapServers = brokerAddress,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            commitProducer = new ProducerBuilder<Null, WindowCommit>(pConfig)
                .SetErrorHandler((_, e) =>
                {
                })
                .Build();

            commitConsumer = new ConsumerBuilder<Null, WindowCommit>(cConfig)
                .SetErrorHandler((_, e) =>
                {
                })
                .Build();
        }

        async public Task CommitAsync(int partition, long offset, long windowId)
        {
            await commitProducer.ProduceAsync(
                commitTopic,
                new Message<Null, WindowCommit>
                {
                    Value = new WindowCommit(partition, offset, windowId)
                });
        }

        async public Task<OffsetWindowId> LastCommittedAsync(int partition)
        {
            await Task.Delay(0);
            return new OffsetWindowId(0, 0);
        }

        public void Dispose()
        {
            commitProducer.Dispose();
            commitConsumer.Dispose();
        }
    }
}