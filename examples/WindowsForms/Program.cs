using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.Examples.AvroSpecific;


namespace WindowsForms
{
    static class Program
    {
        public static IProducer<string, User> Producer { get; set; }

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            var producerConfg = new ProducerConfig
            {
                BootstrapServers = "127.0.0.1:9092"
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = "127.0.0.1:8081"
            };

            CachedSchemaRegistryClient schemaRegistryClient = null;
            try
            {
                schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
                Producer = new ProducerBuilder<string, User>(producerConfg)
                    .SetValueSerializer(new AvroSerializer<User>(schemaRegistryClient))
                    .Build();

                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new MainWindow());
            }
            finally
            {
                Producer.Dispose();
                schemaRegistryClient.Dispose();
            }
        }
    }
}
