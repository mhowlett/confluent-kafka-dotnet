using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public class CommitManager<T> : IDisposable
    {
        string commitTopic;
        IProducer<Null, PartitionAndData> commitProducer;
        IConsumer<Null, PartitionAndData> commitConsumer;
        object lockObj = new object();
        bool isReady = false;
        Dictionary<int, T> lastCommitted = new Dictionary<int, T>();

        public class PartitionAndData
        {
            public PartitionAndData(int partition, T data)
            {
                Partition = partition;
                Data = data;
            }

            public int Partition { get; private set; }
            public T Data { get; private set; }

            class Serializer : ISerializer<PartitionAndData>
            {
                ISerializer<T> dataSerializer;
                public Serializer(ISerializer<T> dataSerializer)
                {
                    this.dataSerializer = dataSerializer;
                }

                public byte[] Serialize(PartitionAndData data, SerializationContext context)
                {
                    var partitionBytes = Serializers.Int32.Serialize(data.Partition, SerializationContext.Empty);
                    var dataBytes = dataSerializer.Serialize(data.Data, SerializationContext.Empty);

                    var result = new byte[partitionBytes.Length + dataBytes.Length];
                    Array.Copy(partitionBytes, result, partitionBytes.Length);
                    Array.Copy(dataBytes, 0, result, partitionBytes.Length, dataBytes.Length);
                    return result;
                }
            }

            public static ISerializer<PartitionAndData> CreateSerializer(ISerializer<T> dataSerializer)
                => new Serializer(dataSerializer);

            class Deserializer : IDeserializer<PartitionAndData>
            {
                IDeserializer<T> dataDeserializer;
                public Deserializer(IDeserializer<T> dataDeserializer)
                {
                    this.dataDeserializer = dataDeserializer;
                }

                public PartitionAndData Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
                {
                    return new PartitionAndData(
                        partition: Deserializers.Int32.Deserialize(data.Slice(0, sizeof(long)), false, SerializationContext.Empty),
                        data: dataDeserializer.Deserialize(data.Slice(sizeof(long)), false, SerializationContext.Empty)
                    );
                }
            }

            public static IDeserializer<PartitionAndData> CreateDeserializer(IDeserializer<T> dataDeserializer)
                => new Deserializer(dataDeserializer);
        }


        public CommitManager(
            string brokerAddress,
            string commitTopic,
            ISerializer<T> dataSerializer,
            IDeserializer<T> dataDeserializer)
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
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            commitProducer = new ProducerBuilder<Null, PartitionAndData>(pConfig)
                .SetValueSerializer(PartitionAndData.CreateSerializer(dataSerializer))
                .SetErrorHandler((_, e) =>
                {
                    // ?
                })
                .Build();

            commitConsumer = new ConsumerBuilder<Null, PartitionAndData>(cConfig)
                .SetValueDeserializer(PartitionAndData.CreateDeserializer(dataDeserializer))
                .SetErrorHandler((_, e) =>
                {
                    // ?
                })
                .Build();

        }

        public async Task MaybeCreateTopicAsync()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = null
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {

            }

            await Task.Delay(0);
        }

        public void Start(CancellationToken cancellationToken)
        {
            StartPollTask(cancellationToken);
        }

        void StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
            {
                try
                {
                    while (true)
                    {
                        var cr = commitConsumer.Consume(ct);
                        lock (lockObj)
                        {
                            if (cr.IsPartitionEOF)
                            {
                                isReady = true;
                                continue;
                            }
                            if (lastCommitted.ContainsKey(cr.Value.Partition))
                            {
                                lastCommitted.Add(cr.Value.Partition, cr.Value.Data);
                            }
                            else
                            {
                                lastCommitted[cr.Value.Partition] = cr.Value.Data;
                            }
                        }
                    }
                }
                catch (OperationCanceledException) {}
            }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        async public Task CommitAsync(int partition, T data)
        {
            await commitProducer.ProduceAsync(
                commitTopic,
                new Message<Null, PartitionAndData>
                {
                    Value = new PartitionAndData(partition, data)
                });
        }

        async public Task<Offset> LastCommittedAsync(int partition)
        {
            await Task.Delay(0);
            return 0;
        }

        public void Dispose()
        {
            commitProducer.Dispose();
            commitConsumer.Dispose();
        }
    }
}