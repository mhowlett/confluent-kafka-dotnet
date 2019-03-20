using System;
using System.Threading.Tasks;
using System.Windows.Forms;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.Examples.AvroSpecific;


namespace WindowsForms
{
    public partial class MainWindow : Form
    {
        CachedSchemaRegistryClient schemaRegistryClient;

        IProducer<string, User> Producer { get; set; }

        IConsumer<string, User> Consumer { get; set; }

        string topicName;


        public MainWindow()
        {
            InitializeComponent();
        }

        private void produceAsyncButton_Click(object sender, EventArgs e)
        {
            if (Producer == null)
            {
                MessageBox.Show("Not connected.");
                return;
            }

            Producer.ProduceAsync(topicName, new Message<string, User> { Key = "hello", Value = new User { name = "windows_user", favorite_color = "blue", favorite_number = 95 } })
                .ContinueWith(r =>
                {
                    resultListBox.Invoke((MethodInvoker)delegate
                    {
                        if (r.IsFaulted)
                        {
                            var ex = (ProduceException<string,User>)r.Exception.InnerException;
                            resultListBox.Items.Insert(0, $"ProduceAsync error delivering to '{ex.DeliveryResult.TopicPartition}': {ex.Error.Reason}");
                        }
                        else
                        {
                            resultListBox.Items.Insert(0, $"ProduceAsync delivered message to: {r.Result.TopicPartitionOffset}. name: {r.Result.Value.name}");
                        }
                    });
                });
        }

        private void beginProduceButton_Click(object sender, EventArgs e)
        {
            if (Producer == null)
            {
                MessageBox.Show("Not connected.");
                return;
            }

            Producer.BeginProduce(topicName, new Message<string, User> { Key = "hello", Value = new User { name = "windows_user", favorite_color = "blue", favorite_number = 95 } },
                deliveryReport =>
                {
                    resultListBox.Invoke((MethodInvoker)delegate
                    {
                        if (deliveryReport.Error.IsError)
                        {
                            resultListBox.Items.Insert(0, $"BeginProduce error delivering to '{deliveryReport.TopicPartition}': {deliveryReport.Error.Reason}");
                        }
                        else
                        {
                            resultListBox.Items.Insert(0, $"BeginProduce delivered message to: {deliveryReport.TopicPartitionOffset}. name: {deliveryReport.Value.name}");
                        }
                    });
                });
        }

        private void consumeButton_Click(object sender, EventArgs e)
        {
            var consumeResult = Consumer.Consume(TimeSpan.FromSeconds(0));
            resultListBox.Invoke((MethodInvoker)delegate
            {
                if (consumeResult == null)
                {
                    resultListBox.Items.Insert(0, $"no message to consume!");
                }
                else
                {
                    resultListBox.Items.Insert(0, $"consumed message from {consumeResult.TopicPartitionOffset}. name: {consumeResult.Value.name}");
                }
            });
        }

        private void connectButton_Click(object sender, EventArgs e)
        {
            if (Consumer != null) { Consumer.Close(); Consumer.Dispose(); }
            if (Producer != null) { Producer.Dispose(); }
            if (schemaRegistryClient != null) { schemaRegistryClient.Dispose(); }

            var producerConfg = new ProducerConfig
            {
                BootstrapServers = bootstrapServersTextBox.Text
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = schemaRegistryUrlTextBox.Text
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServersTextBox.Text,
                GroupId = "windows-forms-example-group",
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            topicName = topicNameTextBox.Text;

            schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

            Producer = new ProducerBuilder<string, User>(producerConfg)
                .SetValueSerializer(new AvroSerializer<User>(schemaRegistryClient))
                .Build();

            Consumer = new ConsumerBuilder<string, User>(consumerConfig)
                .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistryClient))
                .Build();

            Consumer.Subscribe(topicName);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (components != null)
                {
                    components.Dispose();
                }
                if (Consumer != null)
                {
                    Consumer.Dispose();
                }
                if (Producer != null)
                {
                    Producer.Dispose();
                }
                if (schemaRegistryClient != null)
                {
                    schemaRegistryClient.Dispose();
                }
            }

            base.Dispose(disposing);
        }
    }
}
