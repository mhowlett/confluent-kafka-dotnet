// Copyright 2016-2017 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Collections.Generic;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkProducer
    {
        private static long BenchmarkProducerImpl(
            string bootstrapServers, 
            string topic, 
            int nMessages, 
            int nHeaders,
            bool useDeliveryHandler)
        {
            var nReportInterval = nMessages / 10;

            // Most of the performance benefit of batching is achieved with a
            // linger.ms setting of 5ms and this is preferred in this test over
            // a larger value (which would be slightly more optimal). There are
            // two effects at play that will make a larger linger.ms setting
            // produce worse results here:
            // 1. At the beginning of the test, for a period of linger.ms, no
            //    messages will be on the network and kafka will be idle. i.e.
            //    over the duration of the test, there are idle resources which
            //    would not be case during steady-state production.
            // 2. At the end of the test there may (or may not) be a delay before
            //    the final batch is sent after the final message is produced.
            //    This will result in a systematic error in the result (i.e.
            //    an error that will tend to be the same across runs).
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                QueueBufferingMaxMessages = 2000000,
                RetryBackoffMs = 500,
                LingerMs = 5,
                DeliveryReportFields = "none"
            };

            DeliveryResult firstDeliveryReport = null;

            Headers headers = null;
            if (nHeaders > 0)
            {
                headers = new Headers();
                for (int i=0; i<nHeaders; ++i)
                {
                    headers.Add($"header-{i+1}", new byte[] { (byte)i, (byte)(i+1), (byte)(i+2), (byte)(i+3) });
                }
            }

            using (var producer = new Producer(config))
            {
                Console.WriteLine($"{producer.Name} producing on {topic} " + (useDeliveryHandler ? "[Action<Message>]:" : "[Task]:"));

                byte cnt = 0;
                var val = new byte[100].Select(a => ++cnt).ToArray();

                var stopwatch = new Stopwatch();

                // avoid including connection setup, topic creation time, etc.. in result.
                firstDeliveryReport = producer.ProduceAsync(topic, new Message { Value = val, Headers = headers }).Result;

                stopwatch.Start();
                
                if (useDeliveryHandler)
                {
                    var autoEvent = new AutoResetEvent(false);
                    var msgCount = 0;
                    long lastElapsedMs = 0;
                    Action<DeliveryReport> deliveryHandler = (DeliveryReport deliveryReport) => 
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                        {
                            throw new Exception($"Message delivery failed: {deliveryReport.Error}.");
                        }

                        msgCount += 1;

                        if (msgCount % nReportInterval == 0)
                        {
                            var elapsedMs = stopwatch.ElapsedMilliseconds;
                            Console.WriteLine($"  Produced {nReportInterval} messages in {elapsedMs - lastElapsedMs:F0}ms");
                            Console.WriteLine($"  {nReportInterval / (elapsedMs - lastElapsedMs):F0}k msg/s");
                            lastElapsedMs = elapsedMs;
                        }

                        if (msgCount == nMessages) { autoEvent.Set(); }
                    };

                    for (int i = 0; i < nMessages; i += 1)
                    {
                        while (true)
                        {
                            try
                            {
                                producer.BeginProduce(topic, new Message { Value = val, Headers = headers }, deliveryHandler);
                                break;
                            }
                            catch (KafkaException e)
                            {
                                if (e.Error.Code == ErrorCode.Local_QueueFull) { continue; }
                                throw;
                            }
                        }
                    }

                    autoEvent.WaitOne();
                }
                else
                {
                    // The overhead in managing / checking the Tasks returned from ProduceAsync
                    // is significant. Storing all the tasks then calling Task.WaitAll() 
                    // reduces throughput by ~30-40% in tests on my local machine compared with 
                    // simply calling producer.Flush(). However, it's cheating to not wait
                    // on the Tasks themselves, since otherwise any delivery errors will be ignored.
                    //
                    // Note: since there are no messages in flight when the stopwatch is started,
                    // throughtput will be systematically underestimated compared to the
                    // steady-state throughput. The extent of this effect can be estimated by
                    // looking at the delivery handler tests, where samples of the throughput
                    // as the test progresses are reported. The point at which this effect is
                    // not significant is around ~> 10M messages.
                    //
                    // The alternative - starting the stopwatch timer on the first received delivery
                    // report whilst other messages that are part of the test are also in flight -
                    // will systematically over estimate throughput since all messages in the 
                    // first batch effectively have a round-trip time to the broker of 0ms. In my
                    // testing, the error in doing this had higher variability and was greater
                    // than the approach taken here.
                    var tasks = new Task<DeliveryResult>[nMessages];
                    for (int i = 0; i < nMessages; i += 1)
                    {
                        while (true)
                        {
                            try
                            {
                                tasks[i] = producer.ProduceAsync(topic, new Message { Value = val, Headers = headers });
                                break;
                            }
                            catch (KafkaException e)
                            {
                                if (e.Error.Code == ErrorCode.Local_QueueFull) { continue; }
                                throw;
                            }
                        }
                    }
                    Task.WaitAll(tasks);
                }

                var durationMs = stopwatch.ElapsedMilliseconds;

                Console.WriteLine($"  Total:");
                Console.WriteLine($"    Produced {nMessages} messages in {durationMs:F0}ms");
                Console.WriteLine($"    {nMessages / durationMs:F0}k msg/s");
            }

            return firstDeliveryReport.Offset;
        }

        public static long TaskProduce(string bootstrapServers, string topic, int nMessages, int nHeaders)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nHeaders, false);

        public static long DeliveryHandlerProduce(string bootstrapServers, string topic, int nMessages, int nHeaders)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nHeaders, true);
    }
}
