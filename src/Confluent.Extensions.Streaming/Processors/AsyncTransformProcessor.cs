using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Extensions.Streaming;


namespace Confluent.Extensions.Streaming.Processors
{
    /// <summary>
    ///     A stream processor that enables applying async functions
    ///     to a stream of input messages and writing results to
    ///     an output stream (possibly applying filtering). Ordering
    ///     of output messages can be configured to be either input
    ///     order or task completion order.
    /// </summary>
    public class AsyncTransformProcessor<TInKey, TInValue, TOutKey, TOutValue>
    {
        public string Name { get; set; }

        public string BootstrapServers { get; set; }

        public string InputTopic { get; set; }

        public string OutputTopic { get; set; }

        public IDeserializer<TInKey> InKeyDeserializer { get; set; }

        public IDeserializer<TInValue> InValueDeserializer { get; set; }

        public ISerializer<TOutKey> OutKeySerializer { get; set; }

        public ISerializer<TOutValue> OutValueSerializer { get; set; }

        // It's probably preferable to not expose Confluent.Kafka.LogMessage here.
        // but what? possible fields:
        //   - librdkafka | not
        //   - name
        //   - level
        //   - facility  [not known on error messages, but still useful when known. how to expose?]
        //   - message
        public Action<LogMessage> Logger { get; set; }

        public Func<Message<TInKey, TInValue>, Task<Message<TOutKey, TOutValue>>> Function { get; set; }

        public int MaxOutstanding { get; set; } = 20;

        public ErrorTolerance ConsumeErrorTolerance { get; set; } = ErrorTolerance.None;

        public string DebugContext { get; set; } = null;

        public OutputOrderPolicy OutputOrderPolicy { get; set; }

        private Semaphore funcExecSemaphore;


        class PartitionState
        {
            private TaskAndOffset[] executingFunctions;
            private object funcExecLockObj = new object();

            private TaskAndOffset[] waitingToProduce;
            private object produceLockObj = new object();

            private AsyncTransformProcessor<TInKey, TInValue, TOutKey, TOutValue> processor;

            public PartitionState(AsyncTransformProcessor<TInKey, TInValue, TOutKey, TOutValue> processor)
            {
                this.processor = processor;
                executingFunctions = new TaskAndOffset[processor.MaxOutstanding];
                waitingToProduce = new TaskAndOffset[processor.MaxOutstanding];
            }

            private class TaskAndOffset : IComparable
            {
                public Offset Offset { get; set; }
                
                public Task<Message<TOutKey, TOutValue>> Task { get; set; }

                public int CompareTo(object obj)
                    => Offset < ((TaskAndOffset)obj).Offset ? -1 : Offset > ((TaskAndOffset)obj).Offset ? 1 : 0;
            }

            private void AddToEmptySlot(TaskAndOffset[] array, TaskAndOffset taskOffset)
            {
                // O(N), but low fixed cost => comparable, or better than more
                // sophisticated data structures for relatively small N.
                for (int i=0; i<array.Length; ++i)
                {
                    if (array[i] == null)
                    {
                        array[i] = taskOffset;
                        return;
                    }
                }
                throw new Exception("no slot available");
            }

            private TaskAndOffset RemoveFromSlot(TaskAndOffset[] array, Task task)
            {
                // O(N), but low fixed cost => comparable, or better than more
                // sophisticated data structures for relatively small N.
                for (int i=0; i<array.Length; ++i)
                {
                    if (array[i].Task == task)
                    {
                        var result = array[i];
                        array[i] = null;
                        return result;
                    }
                }
                throw new Exception("task not found");
            }

            private Offset MinimumOffset(TaskAndOffset[] array)
            {
                // O(N), but low fixed cost => comparable, or better than more
                // sophisticated data structures for relatively small N.
                Offset min = long.MaxValue;
                for (int i=0; i<array.Length; ++i)
                {
                    if (array[i] != null)
                    {
                        if (min > array[i].Offset)
                        {
                            min = array[i].Offset;
                        }
                    }
                }
                return min;
            }


            public void HandleConsumedMessage(
                ConsumeResult<TInKey, TInValue> cr,
                IConsumer<TInKey, TInValue> consumer,
                IProducer<TOutKey, TOutValue> producer,
                Semaphore funcExecSemaphore,
                CancellationTokenSource errorCts)
            {
                funcExecSemaphore.WaitOne();

                var task = processor.Function(cr.Message);

                lock (funcExecLockObj)
                {
                    AddToEmptySlot(executingFunctions, new TaskAndOffset { Offset = cr.Offset, Task = task });
                }

                task.ContinueWith(t =>
                {
                    TaskAndOffset finishedTaskAndOffset;
                    Offset minExecutingOffset;
                    lock (funcExecLockObj)
                    {
                        finishedTaskAndOffset = RemoveFromSlot(executingFunctions, t);
                        minExecutingOffset = MinimumOffset(executingFunctions);
                        try
                        {
                            consumer.StoreOffset(new TopicPartitionOffset(cr.TopicPartition, minExecutingOffset));    
                        }
                        catch (KafkaException)
                        {
                            // TODO: what errors can happen here? (document).
                            // What to do? probably log then fail fast.

                            // Logger.Log(e.Message).
                            Console.WriteLine("error storing offset");
                            errorCts.Cancel();
                        }
                    }

                    if (t.IsFaulted)
                    {
                        // TODO: should fail fast.

                        // Logger.Log(...)
                        Console.WriteLine("cancelling internal");
                        errorCts.Cancel();
                    }

                    var result = t.Result;

                    if (processor.OutputOrderPolicy == OutputOrderPolicy.TaskCompletionOrder)
                    {
                        if (result != null)
                        {
                            while (true)
                            {
                                try
                                {
                                    producer.Produce(processor.OutputTopic, result);
                                    break;
                                }
                                catch (ProduceException<TOutKey, TOutValue> e)
                                {
                                    if (e.Error.Code == ErrorCode.Local_QueueFull)
                                    {
                                        producer.Poll(TimeSpan.FromSeconds(10));
                                        continue;
                                    }

                                    // Logger.Log(...)
                                    Console.WriteLine("error producing message " + e.Error.Reason);
                                    errorCts.Cancel();
                                    return;
                                    // https://stackoverflow.com/questions/614266/exceptions-on-threadpool-threads
                                    
                                }
                            }
                        }
                    }
                    else if (processor.OutputOrderPolicy == OutputOrderPolicy.InputOrder)
                    {
                        List<TaskAndOffset> toProduceNow = new List<TaskAndOffset>();

                        lock (produceLockObj)
                        lock (funcExecLockObj)
                        {
                            AddToEmptySlot(waitingToProduce, finishedTaskAndOffset);

                            for (int i=0; i<waitingToProduce.Length; ++i)
                            {
                                if (waitingToProduce[i] != null)
                                {
                                    // Note: the minExecutingOffset value could now be out-of-date
                                    // due to another task having completed since it was calculated.
                                    // This race condition is benign though, since that other task
                                    // continuation will ensure the relevant messages are produced.
                                    if (waitingToProduce[i].Offset < minExecutingOffset)
                                    {
                                        toProduceNow.Add(waitingToProduce[i]);
                                        waitingToProduce[i] = null;
                                    }
                                }
                            }
                        }

                        var toProduceArray = toProduceNow.ToArray();
                        Array.Sort(toProduceArray);

                        foreach (var toProduceTask in toProduceArray)
                        {
                            if (toProduceTask.Task.Result != null)
                            {
                                while (true)
                                {
                                    try
                                    {
                                        producer.Produce(processor.OutputTopic, toProduceTask.Task.Result);
                                        break;
                                    }
                                    catch (ProduceException<TOutKey, TOutValue> e)
                                    {
                                        if (e.Error.Code == ErrorCode.Local_QueueFull)
                                        {
                                            producer.Poll(TimeSpan.FromSeconds(10));
                                            continue;
                                        }

                                        // Logger.Log(...)
                                        Console.WriteLine("error producing message " + e.Error.Reason);
                                        errorCts.Cancel();
                                        return;
                                    }
                                }
                            }
                        }            
                    }

                    // release after removal from executing functions
                    // array & waiting to produce array to ensure there
                    // is room in both.
                    funcExecSemaphore.Release();
                });
            }
        }

        public void Start(int instanceId, CancellationToken cancellationToken = default(CancellationToken))
        {
            funcExecSemaphore = new Semaphore(MaxOutstanding, MaxOutstanding);

            CancellationTokenSource errorCts = new CancellationTokenSource();
            CancellationTokenSource compositeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, errorCts.Token);
            CancellationToken compositeCancellationToken = compositeCts.Token;

            bool aMessageHasBeenProcessed = false;

            var cConfig = new ConsumerConfig
            {
                ClientId = $"{Name}-consumer-{instanceId}",
                GroupId = $"{Name}-group",
                BootstrapServers = BootstrapServers,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            if (DebugContext != null)
            {
                cConfig.Debug = DebugContext;
            }

            var cBuilder = new ConsumerBuilder<TInKey, TInValue>(cConfig);
            if (InKeyDeserializer != null)
            {
                cBuilder.SetKeyDeserializer(InKeyDeserializer);
            }
            if (InValueDeserializer != null)
            {
                cBuilder.SetValueDeserializer(InValueDeserializer);
            }
            if (Logger != null)
            {
                cBuilder.SetLogHandler((_, m) =>
                {
                    // If there is an error callback, will any events ever arrive here?
                    // can we change librdkafka to always log errors to here?
                    Logger(m);
                });
            }

            cBuilder.SetErrorHandler((c, e) =>
            {
                // TODO: How can I make an error event happen?
                // simply specifying an incorrect broker name isn't causing
                // an all brokers down event. why?

                if (e.Code == ErrorCode.Local_AllBrokersDown ||
                    e.Code == ErrorCode.Local_Authentication)
                {
                    if (!aMessageHasBeenProcessed)
                    {
                        // Logger.Log(e);
                        errorCts.Cancel();
                        return;
                    }
                }

                // - Typically, 'Error' events should be considered informational.
                // - Typically you will just want to log them, but note that corresponding
                //   events are not sent to the log handler, so you need to do that in this
                //   handler even if you have a log handler set.
                // - TODO: list a set of events you can expect and what they mean
                //     (even though we just want to log - give reader some idea).
                if (Logger != null)
                {
                    // facility is not known here, right?
                    // can we just change librdkafka to not turn off logging if an error handler set?
                    // note: in the .net client, error_cb is currently always set (noop by default).
                    Logger(new LogMessage(c.Name, SyslogLevel.Error, "unknown", e.Reason));
                }
            });


            var pConfig = new ProducerConfig
            {
                ClientId = $"{Name}-producer-{instanceId}",
                BootstrapServers = BootstrapServers,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none"
            };
            if (DebugContext != null)
            {
                pConfig.Debug = DebugContext;
            }

            var pBuilder = new ProducerBuilder<TOutKey, TOutValue>(pConfig);
            if (OutKeySerializer != null)
            {
                pBuilder.SetKeySerializer(OutKeySerializer);
            }
            if (OutValueSerializer != null)
            {
                pBuilder.SetValueSerializer(OutValueSerializer);
            }
            if (Logger != null)
            {
                pBuilder.SetLogHandler((_, m) =>
                {
                    Logger(m);
                });
            }
            pBuilder.SetErrorHandler((p, e) =>
            {
                if (e.IsFatal)
                {
                    // Logger.Log(e);
                    errorCts.Cancel();
                    return;

                    // idempotent fatal error.
                    //   - another case if different replicas are deleting log segments due to 
                    //     retention at different speeds. low watermark offset changes on differnt replicas.
                    //   - what if bad message, then maybe not guaraneed 1m not produced.
                }

                if (e.Code == ErrorCode.Local_AllBrokersDown ||
                    e.Code == ErrorCode.Local_Authentication)
                {
                    if (!aMessageHasBeenProcessed)
                    {
                        // Logger.Log(e);
                        errorCts.Cancel();
                        return;
                    }
                }

                // TODO: list possible error event types expected and what you might want to do with them.

                if (Logger != null)
                {
                    Logger(new LogMessage(p.Name, SyslogLevel.Error, "unknown", e.Reason));
                }
            });

            var partitionState = new Dictionary<TopicPartition, PartitionState>();

            using (var producer = pBuilder.Build())
            using (var consumer = cBuilder.Build())
            {
                consumer.Subscribe(InputTopic);

                try
                {
                    while (true)
                    {
                        ConsumeResult<TInKey, TInValue> cr;
                        try
                        {
                            cr = consumer.Consume(compositeCancellationToken);
                        }
                        catch (ConsumeException ex)
                        {
                            if (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
                            {
                                // For an in-depth discussion of what to do in the event of deserialization errors, refer to:
                                // https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues
                                
                                if (ConsumeErrorTolerance == ErrorTolerance.All)
                                {
                                    continue;
                                }

                                // Logger.Log(error);
                                errorCts.Cancel(); // no error tolerance.
                            }

                            // TODO: what else can go wrong here, and what to do about it?
                            //  - group coordinator error?
                            //  - operationally I think this should fail fast and have a supervisor restart the microservice.

                            Thread.Sleep(TimeSpan.FromSeconds(10)); // ?? if not fail fast, do we want to sleep and why?
                            continue;
                        }

                        if (!partitionState.ContainsKey(cr.TopicPartition))
                        {
                            partitionState.Add(cr.TopicPartition, new PartitionState(this));
                        }
                        partitionState[cr.TopicPartition].HandleConsumedMessage(cr, consumer, producer, funcExecSemaphore, errorCts);

                        aMessageHasBeenProcessed = true;
                    }
                }
                catch (OperationCanceledException) { }
            }

            if (errorCts.IsCancellationRequested)
            {
                throw new Exception("error occured, and we're failing fast.");
            }
        }

        public Task Run(int instanceId, CancellationToken cancellationToken)
            => Task.Run(() => Start(instanceId, cancellationToken));
    }
}
