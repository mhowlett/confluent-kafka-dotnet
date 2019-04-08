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

            public static SimpleSerializer<PartitionAndData> GetSerializer(SimpleSerializer<T> dataSerializer) =>
                data =>
                {
                    var partitionBytes = SimpleSerializers.Int32(data.Partition);
                    var dataBytes = dataSerializer(data.Data);

                    var result = new byte[partitionBytes.Length + dataBytes.Length];
                    Array.Copy(partitionBytes, result, partitionBytes.Length);
                    Array.Copy(dataBytes, 0, result, partitionBytes.Length, dataBytes.Length);
                    return result;
                };

            public static Deserializer<PartitionAndData> GetDeserializer(Deserializer<T> dataDeserializer) =>
                (data, isNull) =>
                {
                    return new PartitionAndData(
                        partition: Deserializers.Int32(data.Slice(0, sizeof(long)), false),
                        data: dataDeserializer(data.Slice(sizeof(long)), false)
                    );
                };
        }


        public CommitManager(
            string brokerAddress,
            string commitTopic,
            SimpleSerializer<T> dataSerializer,
            Deserializer<T> dataDeserializer)
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
                .SetValueSerializer(PartitionAndData.GetSerializer(dataSerializer))
                .SetErrorHandler((_, e) =>
                {
                    // todo
                })
                .Build();

            commitConsumer = new ConsumerBuilder<Null, PartitionAndData>(cConfig)
                .SetValueDeserializer(PartitionAndData.GetDeserializer(dataDeserializer))
                .SetErrorHandler((_, e) =>
                {
                    // todo
                })
                .Build();

        }

        public async Task MaybeCreateCommitTopicAsync()
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