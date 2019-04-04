using System;
using System.Threading;
using System.Threading.Tasks;
using Bogus;
using Confluent.Kafka;


namespace ProducerBlog_StatelessProcessing
{
    class WebLogLine
    {
        public string IP { get; set; }
        public DateTime Date { get; set; }
        public string Method { get; set; }
        public string UserAgent { get; set; }
        public string Url { get; set; }
        public int Response { get; set; }
        public int Size { get; set; }

        public override string ToString()
        {
            return $"{IP} - - [{Date.ToString("dd/MM/yyyy:HH:mm:ss")} -0700] \"{Method} {Url}\" HTTP/1.1 {Response} {Size} \"-\" \"{UserAgent}\"";
        }
    }

    public class WeblogSimulator
    {
        public static async Task Generate(
            string brokerAddress,
            string weblogStreamName,
            CancellationToken cancellationToken)
        {
            var fk = new Faker<WebLogLine>()
                .RuleFor(w => w.IP, (f, u) => f.Internet.Ip())
                .RuleFor(w => w.Date, (f, u) => f.Date.Between(DateTime.Now - TimeSpan.FromSeconds(3), DateTime.Now))
                .RuleFor(w => w.Method, (f, u) => f.PickRandom(new [] { "GET", "POST", "PUT" }))
                .RuleFor(w => w.Url, (f, u) => f.Internet.UrlWithPath())
                .RuleFor(w => w.Response, (f, u) => f.PickRandom(new [] { 200, 201, 301, 302, 404, 500, 504 }))
                .RuleFor(w => w.Size, (f, u) => (int)f.Random.UInt(20, 5000))
                .RuleFor(w => w.UserAgent, (f, u) => f.Internet.UserAgent());

            var rnd = new Random();

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerAddress,
                EnableIdempotence = true,
                LingerMs = 5,
                DeliveryReportFields = "none"
            };

            using (var producer = new ProducerBuilder<Null, string>(pConfig).Build())
            {
                try
                {
                    while (true)
                    {
                        await producer.ProduceAsync(
                            weblogStreamName,
                            new Message<Null, string> { Value = fk.Generate().ToString() }
                        );

                        // what happens if cancellationToken.
                        await Task.Delay(rnd.Next(0, 5000), cancellationToken);
                    }
                }
                catch (OperationCanceledException ex)
                {
                    // ..
                }
            }

            Console.WriteLine("WeblogSimulator thread exiting.");
        }
    }
}
