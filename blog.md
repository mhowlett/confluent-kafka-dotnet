# Async Stream Processing in C#/.NET

The recent v1.0 release of Confluent's Kafka clients is big news for .NET developers. The .NET API has
had a major overhaul, making it more idiomatic, extensible and easier to use. In this blog post,
we're going to walk through some of the new features of the library in the context of implementing
a lightweight abstraction for a common type of stream processing application - executing async
operations (for example HTTP requests) in response to events in a Kafka topic.


The async processor we're going to build allows you to:

- Maintain input order when producing results, even though tasks may take different times to complete.
- Produce output events in response to all, some or none of the input events (i.e. act as a filter).
- Rate limit the number of operations outstanding at any time.
- Scale horizontally with high performance (this *is* Kafka, afterall!)

A goal of this blog post is to provide a reference implementation of a non-trivial use of the .NET Client - something you can look to when implementing your own non-toy applications. Although we'll be using the .NET client, a lot of the ideas in this article are directly transferrable to other clients that build on the librdkafka C library, including Confluent's [Python](...) and [Golang](...) clients. All code discussed in this article can be found in the .NET client github repo, including the [async stream processor](...) itself and an [example](...) of it's use.

For your convenience, we've also created a [nuget](...) package - `Confluent.Experimental.Streaming` - which provides the async processor class. As the name 'experimental' suggests, this is not intended to be taken as an indication of the future direction of any stream processing library we may make for .NET, and it's also not something that we currently support. [note: i don't agree with this, i think we should release it and support it].

With that disclaimer out of the way, let's take a look at how to use what we're going to make. First, you specify your processing job:

```
var transformProcessor = new AsyncTransformProcessor<Null, string, Null, string>
{
    Name = "processor-name",
    BootstrapServers = brokerAddress,
    InputTopic = "input-topic-name",
    OutputTopic = "output-topic-name",
    OutputOrderPolicy = OutputOrderPolicy.InputOrder,
    ConsumeErrorTolerance = ErrorTolerance.All,
    MaxExecutingTasks = 100,
    Function = async (m) => 
    {
        return new Message<Null, string> { Value = await myHttpRequest(...) };
        // return null to indicate no output.
    }
};
```

And then start it:

```
transformProcessor.Start(instanceId, cancellationToken);
```

And that's it! To scale your processing, simply run multiple instances of your program and let Kafka consumer groups do their magic. More detailed information on using `AsyncTransformProcessor` is available in the example's [readme.md](...). 

For the remainder of this blog post, we're going to dive into the
details of it's implementation.


## Setting up

A nice addition to the 1.0 API are the strongly typed configuration classes. These classes are actually just convenience wrappers around the string/string key value pairs expected by librdkafka - which you can still use if you want. However, the specialized configuration classes give you edit / compile time type validation, intellisense API documentation, and they're directly compatible with the ASP .NET `IConfiguration` `Option` pattern (but that's a story for a future blog post!).

There are many configuration options, but for the most part, the defaults are probably what you want. Here's the complete set of parameters we're using for the consumer:

```
var cConfig = new ConsumerConfig
{
    BootstrapServers = BootstrapServers,
    ClientId = $"{Name}-consumer-{instanceId}",
    GroupId = $"{Name}-group",
    EnableAutoOffsetStore = false,
    AutoOffsetReset = AutoOffsetReset.Latest,
    MaxPollIntervalMs = MaxPollIntervalMs,
    Debug = DebugContext
};
```

A few of these are worth calling out:

- **EnableAutoOffsetStore**: Setting this property to `false` gives you to control over when an offset is eligible to be committed when using librdkafka's auto-offset-commit capability. This pattern is usually the best way to commit offsets, and we'll discuss why in more detail later.

- **Debug**: Librdkafka has a `log_level` configuration property, but in practice this isn't very useful and so to avoid confusion, we don't expose it in the .NET strongly typed config classes. By contrast, the `debug` property is very useful. If you are experiencing problems with the client, you can use this property to enable verbose logging in contexts relevant to your problem, or simply set it to `all`. For more information, refer to the librdkafka [documentation](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#debug-contexts).

- **MaxPollIntervalMs**: This property (new in 1.0) specifies the maximum time between calls to a consumer's `Consume` method before it is assumed to have failed and thrown out of the consumer group. Since the function executed by our processor is user defined, this property is also exposed to allow for arbitrarily long running functions. If you're interested in the finer details of consumer liveness works, take a look at [KIP-62](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread).


Producer:

- **EnableIdempotence**: If set to `true`, every message produced to Kafka is guaranteed to be delivered in-order and exactly once. There is very little performance overhead in enabling this feature. New in 1.0.

- **LingerMs**: This setting controls how long the client will wait before batching up messages that have been produced and sending them to the cluster. The default value is zero, i.e. messages are sent immediately without batching. This is optimal in low throughput scenarios, but at higher throughputs you will actually see better latency by increasing `LingerMs` because the number of broker requests decreases, in turn reducing load on the brokers. Maximum throughput is also significantly higher too, of course. Empirically, we have found that a good general purpose setting for `LingerMs` is `5`.

Having specified your configuration, the next stage of client setup is to pass this into the constructor of a client builder class. The builder classes allow you to optionally specify one of a number of different event handlers, as well as serializers or deserializers (collectively known as '*serdes*'). This is done via 'Set' methods which can be chained together. e.g.:

```
builder
```

In our async processor class, we allow the log handler specified, and pass it through to the producer and consumer builder classes if so. If no log handler is specified, output will be written to stderr by default (note: logging will be very sparse unless you specify a debug context). If you specify your own log handler, be aware that it must be threadsafe.

Liewise, we allow deserializers .. and serializer .. however, alos pased through.

Finally, we specify implementations of the error handler and partition assigned handler which are internal. more on those in a bit.

## The Implementation

The first thing to point out is that our processor makes use of just one Producer and one Consumer instance. This is typical -
as a general rule, if you find yourself creating many client instances, you should take a step back and try to think how not to. First, the clients are very performant - can deliver hundreds of thousands of events per second. Also, since performance improves with batching, you prefer high client utilization. Fewer clients also minimizes the number of open TCP connections, though this doesn't become a meaningful constraint until you get to thousands of open connections - batching is where the real win comes from.

After building the consumer and producer, we subscribe tn

Note that sbscrbbe for cg. it's possible to .. only if you need high scalability and dynamic.

At the heart of our async processor is a [single threaded consume loop](...). This pattern might seem low level and out of place in a language like C#, but it's very effective for writing high throughput streaming applications, and standard practice for doing so across many languages. If your application calls for it, you could easily build a higher level abstraction on top of this (and we are likely to provide higher level abstractions as well in the future).

Here's a sligtly minimized version of the loop in our processor class:

```
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
            if (ConsumeErrorTolerance == ErrorTolerance.All)
            {
                continue;
            }

            // Log error
            errorCts.Cancel();
        }

        // Log error
        throw;
    }

    PerPartitionState.HandleConsumedMessage(this, cr, consumer, producer, funcExecSemaphore, errorCts);

    aMessageHasBeenProcessed = true;
}
```

The `Consume` method blocks until either 1. A new message is available for delivery to the application 2. the call is cancelled or 3. an error occured (an exception is thrown).

It's important to note that the `Consume` method does not poll the network socket directly. Behind the scenes, librdkafka manages broker fetch requests on background threads (one per broker), deliverying them to the queue `Consume` is qaiting on when available. 

ConsumeException has stuff.
cr has Message. same as produce, same as dr.


. Each consumer is capable of deliverying hundreds of thousands of messages per second
to the application (depending on message size). Behind the scenes. 



Something we're going to cover in a fair amount of detail is error handling. How should you think
about error handling? In what ways can the client fail? How should you respond. 

The interesting part of this problem is the code for managing the exexcuting async operations -
limiting the maximum number that may be executing simultaneously, and ensuring the order that 
results is produced is the same as the input even stream (if desired).




write some code to perform async operations - like 
fetching a web page, doing database queries, or sending email - in response to events in a Kafka 
stream. 




With the release of the v1.0 Confluent Kafka clients, the story for .NET developers has taken a big
leap forward. The API has had a complete makeover, and now provides a number of conveniences not (yet)
available in other clients.

In this blog post, we're going to take the new client for a spin by looking at how to perform async
operations - like fetching a web page, doing a database query or sending an email - in response to
events in a kafka stream. 


And we're going to get into the details - how should you handle errors?
how should you operate the software? this is not a toy example.

Two new projects have been added to the 



Perhaps the most common questions we get from people working outside the JVM ecosystem is "when kafka streams
support coming to <insert programing environmnet here>".





During a rebalance, consumers can’t consume messages, so a rebalance is basically a short window of unavailability of the entire consumer group. Undesirable.

In java, does consumer.poll perform the FETCH request? 

Tip: Store offsets. what happens on rebalance? (assume comitted).

librdkafka: Commit failures when using store offsets? 


be focussed on the implementation of [`AsyncTransformProcessor`](...), which uses a .NET producer
and consumer instance. We've packaged this class up in a nuget package Confluent.Experimental.Streaming
for your convenience, but we don't support. The intent is for you to use this code as the basis for
your own. To use it, simply specify a few high level configuration parameters and your processing function:



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


[The level is intermediate - we'll skip over some of the basics in order to have room for some less commonly discussed ideas.]


Synchronous operations are the enemy of high throughput processing. 
Synchronous operations are the enemy of high throughput processing. 