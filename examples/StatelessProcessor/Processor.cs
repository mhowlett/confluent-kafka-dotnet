using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.Examples.StatelessProcessor
{
    public class Processor<TInKey, TInValue, TOutKey, TOutValue>
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

        public Func<Message<TInKey, TInValue>, Message<TOutKey, TOutValue>> Function { get; set; }

        public ErrorTolerance ConsumeErrorTolerance { get; set; } = ErrorTolerance.None;

        public string DebugContext { get; set; } = null;


        private bool aMessageHasBeenProcessed = false;

        IConsumer<TInKey, TInValue> constructConsumer(string instanceId, CancellationTokenSource errorCts)
        {
            var cConfig = new ConsumerConfig
            {
                ClientId = $"{Name}-consumer-{instanceId}",
                GroupId = $"{Name}-group",
                BootstrapServers = BootstrapServers,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Latest,
                Debug = DebugContext // can set to null
            };

            var cBuilder = new ConsumerBuilder<TInKey, TInValue>(cConfig)
                .SetKeyDeserializer(InKeyDeserializer)
                .SetValueDeserializer(InValueDeserializer)
                .SetLogHandler((_, m) =>
                {
                    // If there is an error callback, will any events ever arrive here?
                    // can we change librdkafka to always log errors to here?
                    Logger(m);
                })
                .SetPartitionsAssignedHandler((c, e) =>
                {
                    // PerPartitionState.Clear();
                })
                .SetErrorHandler((c, e) =>
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

            return cBuilder.Build();
        }

        IProducer<TOutKey, TOutValue> constructProducer(string instanceId, CancellationTokenSource errorCts)
        {
            var pConfig = new ProducerConfig
            {
                ClientId = $"{Name}-producer-{instanceId}",
                BootstrapServers = BootstrapServers,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none",
                Debug = DebugContext
            };

            var pBuilder = new ProducerBuilder<TOutKey, TOutValue>(pConfig)
                .SetKeySerializer(OutKeySerializer)
                .SetValueSerializer(OutValueSerializer)
                .SetLogHandler((_, m) =>
                {
                    Logger(m);
                })
                .SetErrorHandler((p, e) =>
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

            return pBuilder.Build();
        }

        public void Start(string instanceId, CancellationToken cancellationToken)
        {
            CancellationTokenSource errorCts = new CancellationTokenSource();
            

            IConsumer<TInKey, TInValue> consumer = null;
            IProducer<TOutKey, TOutValue> producer = null;

            try
            {
                if (InputTopic != null)
                {
                    consumer = constructConsumer(instanceId, errorCts);
                    consumer.Subscribe(InputTopic);
                }

                if (OutputTopic != null)
                {
                    producer = constructProducer(instanceId, errorCts);
                }

                while (true)
                {
                    Message<TInKey, TInValue> message = null;

                    if (InputTopic != null)
                    {
                        try
                        {
                            // callback handler exceptions don't propagate.
                            message = consumer.Consume(compositeCancellationToken).Message;
                        }
                        catch (ConsumeException ex)
                        {
                            if (ex.Error.Code == ErrorCode.Local_ValueDeserialization ||
                                ex.Error.Code == ErrorCode.Local_KeyDeserialization)
                            {
                                if (ConsumeErrorTolerance == ErrorTolerance.All)
                                {
                                    continue;
                                }

                                // Log then:
                                break; // no error tolerance.
                            }

                            // possible: ErrorCode.Local_UnknownGroup, if rk->rkcg not set.
                            //    - when can that happen?
                            // - Authorization failures. (see java docs)
                            // - Invalid group id.
                            // - other future errors.
                            
                            // - session timeout. 
                            //   - if this occurs, I assume there is no revoke?
                            //   - will consumer try to re-join group?

                            // Log, then:
                            break;
                        }
                    }

                    var result = Function(message);

                    if (result != null)
                    {
                        producer.Produce(OutputTopic, result);
                    }

                    aMessageHasBeenProcessed = true;
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                if (producer != null)
                {
                    producer.Dispose();
                }
                if (consumer != null)
                {
                    consumer.Dispose();
                }
            }

            if (errorCts.IsCancellationRequested)
            {
                throw new Exception("error occured, and we're failing fast.");
            }
        }
    }
}
