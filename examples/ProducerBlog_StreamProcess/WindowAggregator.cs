using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    public class WindowAggregator<TGroup, TIn, TOut>
    {
        int windowSizeSeconds = 30;
        int windowHisteresisSeconds = 5;
        string name;
        string brokerAddress;
        string inputTopic;
        string outputTopic;
        string commitTopic;
        int instanceId;
        IDeserializer<TGroup> groupDeserializer;
        IDeserializer<TIn> inputDeserializer;
        ISerializer<TGroup> groupSerializer;
        ISerializer<TOut> outputSerializer;

        CommitManager<OffsetAndTimestamp> commitManager;
        IProducer<Null, WindowResult<TGroup, TOut>> producer;
        IConsumer<TGroup, TIn> consumer;
        

        Dictionary<long, Dictionary<string, int>> countryCounts;

        public WindowAggregator(
            string brokerAddress,
            string name,
            string inputTopic,
            string outputTopic,
            string commitTopic,
            IDeserializer<TGroup> groupDeserializer,
            IDeserializer<TIn> inputDeserializer,
            ISerializer<TGroup> groupSerializer,
            ISerializer<TOut> outputSerializer,
            TimeSpan windowSize,
            TimeSpan windowHisteresis,
            int instanceId)
        {
            this.windowSizeSeconds = (int)windowSize.TotalSeconds;
            this.windowHisteresisSeconds = (int)windowHisteresis.TotalSeconds;
            this.brokerAddress = brokerAddress;
            this.name = name;
            this.inputTopic = inputTopic;
            this.outputTopic = outputTopic;
            this.commitTopic = commitTopic;
            this.groupDeserializer = groupDeserializer;
            this.inputDeserializer = inputDeserializer;
            this.groupSerializer = groupSerializer;
            this.outputSerializer = outputSerializer;
            this.instanceId = instanceId;
        }

        public async Task Run(
            Func<TGroup, TIn, TOut> agFunc,
            CancellationToken cancellationToken)
        {
            var cConfig = new ConsumerConfig
            {
                ClientId = $"{name}-consumer-{instanceId}",
                GroupId = $"{name}-group",
                BootstrapServers = brokerAddress,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var pConfig = new ProducerConfig
            {
                ClientId = $"{name}-producer-{instanceId}",
                BootstrapServers = brokerAddress,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none"
            };

            using (commitManager = new CommitManager<OffsetAndTimestamp>(
                brokerAddress,
                commitTopic,
                new OffsetAndTimestamp.Serializer(),
                new OffsetAndTimestamp.Deserializer()))

            using (producer =
                new ProducerBuilder<Null, WindowResult<TGroup, TOut>>(pConfig)
                    .SetValueSerializer(WindowResult<TGroup, TOut>.CreateSerializer(groupSerializer, outputSerializer))
                    .SetErrorHandler((_, e) =>
                    {
                        // ?
                    })
                    .Build())
            
            using (consumer =
                new ConsumerBuilder<TGroup, TIn>(cConfig)
                    .SetKeyDeserializer(groupDeserializer)
                    .SetValueDeserializer(inputDeserializer)
                    .SetPartitionsAssignedHandler((_, ps) =>
                    {
                        // async handlers are not currently supported.
                        return ps.Select(topicPartition => new TopicPartitionOffset(
                            topicPartition,
                            commitManager.LastCommittedAsync(topicPartition.Partition).GetAwaiter().GetResult()
                        ));
                    })
                    .SetErrorHandler((_, e) =>
                    {
                        // ?
                    })
                    .Build())
            {
                await commitManager.MaybeCreateCommitTopicAsync();

                consumer.Subscribe(inputTopic);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cancellationToken);
                            // smarts. realize when we at boundary, and commit anything.
                            agFunc();
                            // Process(cr, outputTopic);
                        }
                        catch (ConsumeException ex)
                        {
                            if (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
                            {
                                continue;
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }

            Console.WriteLine("WeblogSimulator.Process method terminated.");
        }
    }
}
