using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.Extensions.Streaming.Processors
{
    public class TransformProcessor<TInKey, TInValue, TOutKey, TOutValue>
    {
        // TODO: this is a much simpler version of AsyncTransformProcessor.
        // We should implement this now since it's simple and useful in the
        // example (for printing out processed messages).
    }
}
