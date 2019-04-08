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
        string name;
        string brokerAddress;
        string inputTopic;
        string outputTopic;
        string commitTopic;
        Deserializer<TGroup> groupDeserializer;
        Deserializer<TIn> inputDeserializer;
        SimpleSerializer<TGroup> groupSerializer;
        SimpleSerializer<TOut> outputSerializer;
        long windowSizeMilliseconds;
        long emitDelayMilliseconds;
        int instanceId;

        CommitManager<OffsetAndTimestamp> commitManager;
        IProducer<Null, WindowResult<TGroup, TOut>> producer;
        IConsumer<TGroup, TIn> consumer;
        
        class PartitionWindowState
        {
            long windowSizeMilliseconds;
            long emitDelayMilliseconds;
            long latestTime = long.MinValue;

            public PartitionWindowState(long windowSizeMilliseconds, long emitDelayMilliseconds)
            {
                this.windowSizeMilliseconds = windowSizeMilliseconds;
                this.emitDelayMilliseconds = emitDelayMilliseconds;
                windowData = new List<Dictionary<TGroup, List<Message<TGroup, TIn>>>>();
                minWindowOffsets = new List<Offset>();
            }

            public bool AddData(Timestamp time, TGroup group, ConsumeResult<TGroup, TIn> cr)
            {
                if (time.UnixTimestampMs > latestTime)
                {
                    latestTime = time.UnixTimestampMs;
                }

                var window = time.UnixTimestampMs / windowSizeMilliseconds;

                if (windowData.Count == 0)
                {
                    firstWindow = window;
                    windowData.Add(new Dictionary<TGroup, List<Message<TGroup, TIn>>>());
                    minWindowOffsets.Add(cr.Offset);
                }

                var index = (int)(window - firstWindow);

                // data corresponds to window that has already been closed off - discard.
                if (index < 0)
                {
                    return false;
                }

                // add windows as necessary.
                // TODO: If this is too many, then do something smart, probably delegate
                //       to caller by throwing exception. caller can process, then recall
                //       add data.
                for (int i=0; i<=index-windowData.Count; ++i)
                {
                    windowData.Add(new Dictionary<TGroup, List<Message<TGroup, TIn>>>());
                    minWindowOffsets.Add(cr.Offset);
                }

                if (!windowData[index].ContainsKey(group))
                {
                    windowData[index].Add(group, new List<Message<TGroup, TIn>>());
                }
                windowData[index][group].Add(cr.Message);

                return true;
            }

            public List<List<WindowResult<TGroup, TOut>>> Process(Func<List<Message<TGroup, TIn>>, Timestamp, TimeSpan, TOut> f, out long lastClosedOffWindow)
            {
                var emitIfAfter = this.firstWindow * this.windowSizeMilliseconds + this.emitDelayMilliseconds;
                if (latestTime < emitIfAfter)
                {
                    lastClosedOffWindow = long.MinValue;
                    return null;
                }

                var numberToEmit = (latestTime - emitIfAfter) / windowSizeMilliseconds;
                var result = new List<List<WindowResult<TGroup, TOut>>>();
                for (int i=0; i<numberToEmit; ++i)
                {
                    var wResults = new List<WindowResult<TGroup, TOut>>();
                    foreach (var group in windowData[i].Keys)
                    {
                        var ts = new Timestamp(this.firstWindow * this.windowSizeMilliseconds*i, TimestampType.NotAvailable);
                        TOut v = f(windowData[i][group], ts, TimeSpan.FromMilliseconds(this.windowSizeMilliseconds));
                        wResults.Add(new WindowResult<TGroup, TOut>(ts, group, v));
                    }
                    result.Add(wResults);
                }

                lastClosedOffWindow = ;
                return result;
            }

            public long firstWindow;
            public List<Dictionary<TGroup, List<Message<TGroup, TIn>>>> windowData;
            public List<Offset> minWindowOffsets;
        }

        Dictionary<int, PartitionWindowState> perPartitionState = new Dictionary<int, PartitionWindowState>();

        public WindowAggregator(
            string brokerAddress,
            string name,
            string inputTopic,
            string outputTopic,
            string commitTopic,
            Deserializer<TGroup> groupDeserializer,
            Deserializer<TIn> inputDeserializer,
            SimpleSerializer<TGroup> groupSerializer,
            SimpleSerializer<TOut> outputSerializer,
            TimeSpan windowSize,
            TimeSpan emitDelay,
            int instanceId)
        {
            this.brokerAddress = brokerAddress;
            this.name = name;
            this.inputTopic = inputTopic;
            this.outputTopic = outputTopic;
            this.commitTopic = commitTopic;
            this.groupDeserializer = groupDeserializer;
            this.inputDeserializer = inputDeserializer;
            this.groupSerializer = groupSerializer;
            this.outputSerializer = outputSerializer;
            this.windowSizeMilliseconds = (long)windowSize.TotalMilliseconds;
            this.emitDelayMilliseconds = (long)emitDelay.TotalMilliseconds;
            this.instanceId = instanceId;
        }

        public async Task Run(
            Func<List<Message<TGroup, TIn>>, Timestamp, TimeSpan, TOut> f,
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
                OffsetAndTimestamp.GetSerializer(),
                OffsetAndTimestamp.GetDeserializer()))

            using (producer =
                new ProducerBuilder<Null, WindowResult<TGroup, TOut>>(pConfig)
                    .SetValueSerializer(WindowResult<TGroup, TOut>.CreateSerializer(groupSerializer, outputSerializer))
                    .SetErrorHandler((_, e) =>
                    {
                        // todo
                    })
                    .Build())

            using (consumer =
                new ConsumerBuilder<TGroup, TIn>(cConfig)
                    .SetKeyDeserializer(groupDeserializer)
                    .SetValueDeserializer(inputDeserializer)
                    .SetPartitionsAssignedHandler((_, ps) =>
                    {
                        try
                        {
                            perPartitionState = new Dictionary<int, PartitionWindowState>();
                            foreach (var p in ps)
                            {
                                perPartitionState.Add(p.Partition, new PartitionWindowState(windowSizeMilliseconds, emitDelayMilliseconds));
                            }

                            return ps.Select(topicPartition => new TopicPartitionOffset(
                                topicPartition,
                                // async handlers are not currently supported.
                                commitManager.LastCommittedAsync(topicPartition.Partition).GetAwaiter().GetResult()
                            ));
                        }
                        catch
                        {
                            // todo: flag error.
                            return ps.Select(topicPartition => new TopicPartitionOffset(topicPartition, Offset.Unset));
                        }
                    })
                    .SetErrorHandler((_, e) =>
                    {
                        // todo
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
                            var partitionState = perPartitionState[cr.Partition];
                            partitionState.AddData(cr.Timestamp, cr.Key, cr.Message);
                            var processed = partitionState.Process(f, out long lastClosedOffWindow);
                            if (processed == null)
                            {
                                continue;
                            }

                            var tasks = new List<Task>();
                            foreach (var p in processed)
                            {
                                foreach (var w in p)
                                {
                                    Console.WriteLine("producing" + w.Key+ " " + w.Value);
                                    tasks.Add(producer.ProduceAsync(
                                        outputTopic,
                                        new Message<Null, WindowResult<TGroup, TOut>> { Value = w }));
                                }
                            }

                            Task.WaitAll(tasks.ToArray());

                            commitManager.CommitAsync(cr.Partition, new OffsetAndTimestamp())
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
