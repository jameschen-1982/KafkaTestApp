using Confluent.Kafka;
using Microsoft.Data.SqlClient;

namespace KafkaProducer;

class Program
{
    static async Task Main(string[] args)
    {
        var conf = new ProducerConfig { BootstrapServers = "localhost:9092" };

        Action<DeliveryReport<string, string>> handler = r =>
            Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery Error: {r.Error.Reason}");

        using (var p = new ProducerBuilder<string, string>(conf).Build())
        {
            for (int i = 0; i < 100; ++i)
            {
                p.Produce("NewTopic", new Message<string, string> { Key = i.ToString(), Value = i.ToString() }, handler);
            }

            // wait for up to 10 seconds for any inflight messages to be delivered.
            p.Flush(TimeSpan.FromSeconds(10));
        }
    }
}