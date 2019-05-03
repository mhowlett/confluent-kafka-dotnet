# Stateless Stream Processing in C#/.NET

The recent v1.0 release of Confluent's Kafka clients is big news for .NET developers.
The .NET API has had a major overhaul, making it more idiomatic and extensible, setting a great foundation for future development. In this blog post, we're going to walk through some of the new features of the library in the context of implementing a high level abstraction for stateless stream processing.

The primary goal of this blog post is to provide some in-depth commentary on how to use the library that you can look to when implementing your own applications. Compared to interacting with the Kafka protocol itself, the .NET client is very high level. The Kafka protocol pushes a lot of complexity onto the clients. But the library allows for a lot of flexibility, and there is still quite a bit of work involved in using it well, and we'll be covering the typical patterns for doing so.

In practice, you'll often want to implement your own high level abstractions on top of the clients - like we're doing here with the stateless stream processor. In the future, we anticipate providing more of these high level abstractions out-of-the-box.

Although we'll be using the .NET client, a lot of the ideas in this article are directly transferrable to other clients that build on the librdkafka C library, including Confluent's [Python](...) and [Golang](...) clients. All code discussed in this article can be found in the .NET client github repo, including the [stream processor](...) itself and an [example](...) of it's use.

Here's an example of how to use the `Processor` class we're going to make:

```
var processor = new Processor<Null, string, string, string>
{
    Name = "weblog-processor",
    BootstrapServers = brokerAddress,
    InputTopic = "weblog-topic",
    OutputTopic = "pii-compliant-weblog",
    ConsumeErrorTolerance = ErrorTolerance.All,
    Function = (inMessage) => 
    {
        var country = geoLookup(extractIp(inMessage.Value));
        var piiCompliant = removeIp(inMessage.Value);
        return new Message<string, string> { Key = country, Value = piiCompliant };
    }
};
```

The `Function` does som takes messages containing a web server log, removes the ip address to make the web log PII compliant, and repartitions by country.

Just set a few high level configuration properties (including your processing function) and start it:

```
transformProcessor.Start(instanceId, cancellationToken);
```

... and that's it!

To scale your processing, simply run multiple instances of your program (specifying different instanceId's) and let the Kafka consumer group do it's magic.

More in depth information on how to use the `Processor` class is available in the example's [readme.md](...). For the remainder of this blog post is concerned with it's [implementation](...). Some basic knowledge of kafka clients is assumed, including how consumer groups and offsets work. If you need a refresher, check out [this blog post](...) by [dsf](..). 


## Setting up

A nice addition to the 1.0 API are the strongly typed configuration classes. These classes are just convenience wrappers around the string/string configuration settings [expected by librdkafka](...), which you can still use if you want. However, the specialized configuration classes give you edit and compile time type validation, API documentation via intellisense, and they're directly compatible with the ASP.NET [options pattern](https://docs.microsoft.com/en-us/aspnet/core/fundamentals/configuration/options) - but that's a story for a future blog post!

There are many configuration options, but for the most part, the defaults are probably what you want. Here's all properties we set for the consumer:

```
var cConfig = new ConsumerConfig
{
    BootstrapServers = BootstrapServers,
    ClientId = $"{Name}-consumer-{instanceId}",
    GroupId = $"{Name}-group",
    EnableAutoOffsetStore = false,
    EnableAutoCommit = true,
    AutoOffsetReset = AutoOffsetReset.Latest,
    MaxPollIntervalMs = MaxPollIntervalMs,
    Debug = DebugContext,
    LogNotifiedErrors = true
};
```

A few of these configuration parameters, in particular `EnableAutoOffsetStore` and `EnableAutoCommit` are closely tied to the application logic, and we'll cover them later in this blog post. It rarely makes sense for these properties to be configured independently of the appliction code, so if your application allows client settings to be specified in a configuration file, you'll usually need to do some validation - sometimes with other settings as well (dependening on the application). We don't allow this in our example, but it's a common pattern.

Some consumer configuration properties worth calling out:

- **AutoOffsetReset**: The important thing to realize about this property is it defines the offset the consumer should start consuming from *only if there are no offsets already committed for a partition for the consumer group*. If there are committed offsets, those will be used instead.

- **MaxPollIntervalMs**: This property allows you to specify the maximum time between calls to a consumer's `Consume` method before it is assumed to have failed and thrown out of the consumer group. This property is new in 1.0, and improves the previous functionality. If you're interested in the finer details of consumer liveness works, take a look at [KIP-62](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread).

- **Debug**: Librdkafka has a `log_level` configuration property, but in practice this isn't very useful and so to avoid confusion, we don't expose it in the .NET strongly typed config classes. By contrast, the `debug` property is very useful. If you are experiencing problems with the client, you can use this property to enable verbose logging in contexts relevant to your problem, or simply set it to `all`. For more information, refer to the librdkafka [documentation](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#debug-contexts).

For the Producer:

- **EnableIdempotence**: Setting this parameter to true, guarantees every message produced to Kafka is delivered in-order and exactly once. There is little performance overhead in enabling this feature, and little reason to not enable this. For more information [blog post](...).

- **LingerMs**: This setting controls how long the client will wait before batching up messages that have been produced and sending them to the cluster. The default value is zero, i.e. messages are sent immediately without batching. This is optimal in low throughput scenarios, but at higher throughputs you will actually see better latency by increasing `LingerMs` because the number of broker requests decreases, in turn reducing load on the brokers. Maximum throughput is also significantly higher too, of course. Empirically, we have found that a good general purpose setting for `LingerMs` is `5`.

Having specified your configuration properties, the next stage of client setup is to pass them into the constructor of a client builder class. The builder classes are used to specify various callback methods, specifically event handlers and serializers or deserializers (collectively known as '*serdes*'). 

From our example:

```
var cBuilder = new ConsumerBuilder<TInKey, TInValue>(cConfig)
    .SetKeyDeserializer(InKeyDeserializer)
    .SetValueDeserializer(InValueDeserializer)
    .SetLogHandler(Logger)
    .SetErrorHandler((c, e) =>
    {
        ...
    });
```

It's permissible to specify `null` in any of the builder setter methods, and the call will be ignored. Conveniently, this allows us to just pass in the `InKeySerializer`, `InValueSerializer` and `Logger` properties directly from our `Processor` class, which are `null` by default.

If you don't specify a logger, the default behavior is for log messages to be written to `stderr`. Setting a log handler overrides this default behavior. If you don't specify deserializers, default deserializers will be used corresponding to the input message key and value types `TInKey` and `TInValue` where these are available. Default deserializers are provided for `string` (UTF8 encoding), `int`, `long`, `float`, `double` (network byte order encoding), `Null` (throws exeption if data isn't null) and `Ignore` (returns null for any value).

Finally, we implement an error handler - we'll talk about the details of this later when we talk about error handling.


## The Processing Loop

Our `Processor` makes use of just one `Producer` and one `Consumer` instance. This is typical. The clients are very performant - can deliver hundreds of thousands of events per second and CPU is typically not a bottleneck. Additionally, efficiency improves with higher client utilization because of increased batching of messages in communication with the cluster. Fewer clients also minimizes the number of open TCP connections, though this doesn't become a meaningful constraint until you get to thousands of connections.

At the heart of our processor is a [single threaded consume loop](...). This pattern is quite low level for a language like C#, but it's effective for writing high throughput streaming applications and standard practice for doing so across many languages. 

Here's the produce loop in our processor class:

```
while (!compositeCts.IsCancellationRequested)
{
    ConsumeResult<TInKey, TInValue> cr = null;

    if (InputTopic != null)
    {
        try
        {
            // callback handler exceptions don't propagate.
            cr = consumer.Consume(compositeCts.Token);
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

    var result = Function(cr == null ? null : cr.Message);

    if (result != null)
    {
        while (true)
        {
            try
            {
                producer.Produce(OutputTopic, result,
                    d =>
                    {
                        if (d.Error.Code != ErrorCode.NoError)
                        {
                            errorCts.Cancel();
                        }
                        if (cr != null)
                        {
                            consumer.StoreOffset(cr);
                        }
                    });
                break;
            }
            catch (KafkaException e)
            {
                if (e.Error.Code == ErrorCode.Local_QueueFull)
                {
                    producer.Poll(TimeSpan.FromSeconds(1));
                }
            }
        }
    }
    else
    {
        consumer.StoreOffset(cr);
    }

    aMessageHasBeenProcessed = true;
}
```

There is quite a lot of code here! Most of it relates to error handling, which we'll cover in the next section. Here, we'll just walk through the happy-case.

### Consuming

The first thing we do is call the `Consume` method (line X). This blocks until either 1. A new message is available for delivery to the application 2. the call is cancelled or 3. an error occured (an exception is thrown).

In contrast to the `poll` method in the Java client, the `Consume` method only returns one message at a time to the application. Also, fetch requests to the cluster do not occur as a direct result of calls to the `Consume` method. librdkafka is in control of this process completely, maintaining a dedicate thread for each broker that is the leader of one or more partitions it is consuming from. These threads pass messages to a central queue as they are fetched and it is this queue that the `Consume` method is waiting on for new messages.

The `ConsumeResult` instance returned by the `Consume` method always corresponds to a successfuly consumed message (unless you've enabled partition eof notifications, which are also exposed via ..). Any errors during consume result in a `ConsumeException` being thrown. The purpose of the `ConsumeResult` class is to add context to the `Message` class, which represents the message payload. In particular, the partition consumed from and the offest in that partition.

we flag that we have processed a message. This is useful in our error handling.

### Processing and Producing

Assuming there is no error, we apply the user configured `Function` to the consumed message (line X) and then write the result using the producer (line X). The produce call is wrapped in a retry loop (line X), but this only comes into play in an error scenario (discussed in the next section) due to the `break` statement on line X.

The produce call is non-blocking - does not wait for a response from the cluster to acknowledge successful production of the message (or otherwise). Program flow immediately continues on to the next iteration of the main loop, consuming another message, processing it, and writing out the result. In practice, our consume loop can execute hundreds of thousands of times per second, and there can be an equally large number of produce requests in flight.

When the outcome of a produce call (a 'delivery report') becomes available, this is exposed to the application via the callback passed in as the third argument of the produce call. The .NET Client actually provides two methods for producing messages - `Produce`, which provides message delivery notifications via a callback function and `ProduceAsync`, which uses `Task`s for this purpose. We prefer `Produce` in this scenario primarily because it's more efficient (up to 2x the throughput, in the case of very small messages). `ProduceAsync` is useful where you have high concurrency in the calling function, such as in a web request handler.

Another subtle difference between the two methods is that all `Produce` callbacks execute on a single thread, and the order of broker originated results is guaranteed to match execution order of the callbacks. This is not necessarily guaranteed with the `Task` results, which may typically continue on any thread pool thread.

### Committing

Our callback function is specified inline and makes use of the `cr` value that was set earlier in the loop. When the callback is called, the value of `cr` will be the same as when the `Produce` function was called, even though the callback is executing on a different thread and the value of `cr` in the main processing loop is likely to have changed in the mean time. This is an example of a *closure* and we say the `cr` value has been *captured*.

In the produce callback, we would like to commit the offset corresponding to the consumed message, since it has now been completely processed. Since we've confiured idempotence, we are guaranteed all messages are produced in order and exactly once and that our callback will be.
We could use the `Commit` method to do this, but that is a synchronous operation which would block the callback thread, limiting the speed at which we are able to process delivery reports.

Instead, we make use of librdkafka's auto-commit capability. When `EnableAutoCommit` is set to `true`, a background thread priodically commits the last offset which has been *stored* for each partition of interest. Usually, this offset is set automatically just before the message is delievered to the application by `Consume`, however we can turn this behaior off by setting the `EnableAutoOffsetStore` config property to `false`. We can then explicitly specify the offset that the commit thread commits using the `StoreOffsets` method.

The default behavior gives at-most once semantics - if the application crashes between the message being delivered to the application and the message being processed, the offset may still be committed, and the message will not be processed when the application is restarted. By waiting to call `StoreOffsets` until the result has been confirmed to have been produced, we achieve at least once semantics.

The downside of the periodic-commit approach is that if the application is shutdown uncleanly, there 


## Error handling

people often ask: "how can I check if my cluster is down?"

This question is not as simple as it sounds. What does 'cluster down' mean? Is that all brokers down? Or just the leaders for the partitions you care about? Does it include the replicas? If all brokers are down is this just a temporary networking problem? Also, assuming we did propagate broker state information via the client, should we also make partition leader information available, and maybe consumer coordinator information?

There is a lot of complexiy in 
, and as much as possible, librdkafka abstracts all of this away from you. It assumes all problems are temporary and attempts to recover from them automatically. Generally this is what you want, however the client provides some hooks for you to react to problems and we discuss these in the following sections.

Let's start with the main processing loop. 

### Consume exceptions

The consume method can throw an exception in hte following scenarios:

- deserialization error.
- ...

The only one non-fatal is deserialization.
- ignore all
- ignore none
- dead-letter queue.

### Produce exceptions

Although the produce call is asynchronous, there are some circumstances in which it can produce an error immediately. These are:

- local queue full
- ...

The only error that should be considered non-fatal is 

### Delivery Report Errors

### Error handler




During a rebalance, consumers can’t consume messages, so a rebalance is basically a short window of unavailability of the entire consumer group. Undesirable.

Tip: Store offsets. what happens on rebalance? (assume comitted).

librdkafka: Commit failures when using store offsets? 


rates. abstracts away stuff.

assuming confiured everything correctly.
 - any errors re by definition temporry.
 - number of retries not very useful.
 - only thing how long is this data valid to be produced.

 - librdkakfa thundering herd.
 - reconnect back off.
 - backoff on produce reuests.

 - data to error raet. if that goes towards more errors 
 - could raise fatal error on this.

 - make always log by default, but have config to thundering
   that off. annoying to have error if handled, but in general
   shouldn't want that.


 Another difference worth mentioning is that all `Produce` callbacks are serviced by a single thread, and the order in which the callbacks is called is guaranteed to be the same as the order in which delivery reports are available. By contrast, `Task`s returned by `ProduceAsync` are completed on thread pool threads, so order of continuations is not guaranteed.

  The client also provides a `Commit` method. Why not use this? because `Commit` is synchronous and synchronous operatons are the enemy of high throughput processing. 
