using System.Data;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;

namespace KafkaConsumer;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("NewTopic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    // Prevent the process from terminating.
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");

                            await using SqlConnection connection = new SqlConnection(
                                "Data source=localhost; Initial Catalog=kafka_test; User Id=kafka_user; Password=Abc123456!;TrustServerCertificate=True;");
                            Console.WriteLine("\nQuery data example:");
                            Console.WriteLine("=========================================\n");

                            var sql = "InsertUser";

                            await using SqlCommand cmd = new SqlCommand(sql, connection);
                            cmd.CommandType = CommandType.StoredProcedure;

                            cmd.Parameters.Add("@FirstName", SqlDbType.VarChar).Value = "John";
                            cmd.Parameters.Add("@LastName", SqlDbType.VarChar).Value = "Smith";

                            await connection.OpenAsync(cts.Token);
                            await cmd.ExecuteNonQueryAsync(cts.Token);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
}