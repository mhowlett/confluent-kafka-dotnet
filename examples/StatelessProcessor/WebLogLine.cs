using System;
using System.Threading.Tasks;
using Bogus;


namespace Confluent.Examples.StatelessProcessor
{
    public class WebLogLine
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

        static Faker<WebLogLine> fk = new Faker<WebLogLine>()
            .RuleFor(w => w.IP, (f, u) => f.Internet.Ip())
            .RuleFor(w => w.Date, (f, u) => f.Date.Between(DateTime.Now - TimeSpan.FromSeconds(3), DateTime.Now))
            .RuleFor(w => w.Method, (f, u) => f.PickRandom(new [] { "GET", "POST", "PUT" }))
            .RuleFor(w => w.Url, (f, u) => f.Internet.UrlWithPath())
            .RuleFor(w => w.Response, (f, u) => f.PickRandom(new [] { 200, 201, 301, 302, 404, 500, 504 }))
            .RuleFor(w => w.Size, (f, u) => (int)f.Random.UInt(20, 5000))
            .RuleFor(w => w.UserAgent, (f, u) => f.Internet.UserAgent());

        public static string GenerateFake()
        {
            return fk.Generate().ToString();
        }
    }
}
