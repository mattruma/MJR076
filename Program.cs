using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Configuration;
using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace MJR076
{
    class Program
    {
        // https://www.c-sharpcorner.com/article/azure-event-hub-implementation-using-net-core-console-app/
        // https://www.thecodebuzz.com/parse-command-line-argument-system-commandline/

        protected static IConfiguration _configuration;

        public static async Task Main(string[] args)
        {
            var rootCommand = new RootCommand
            {
                new Option<int>(
                    "--number-of-batches",
                    "The number of batches to send."),
                new Option<int>(
                    "--number-of-messages",
                    "The number of messages to send per batch.")
            };

            rootCommand.Description = "Console App to load test an Azure Event Hub.";

            _configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
                .Build();

            Console.ResetColor();
            Console.WriteLine("Event Hub Load Tester");

            rootCommand.Handler = CommandHandler.Create<int, int>(ExecuteAsync);

            await rootCommand.InvokeAsync(args);

            Console.ReadKey();
        }

        public static async void ExecuteAsync(
            int numberOfBatches,
            int numberOfMessages)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            
            Console.WriteLine();
            Console.WriteLine("# Batches            : {0:N0}", numberOfBatches);
            Console.WriteLine("# Messages/Batch     : {0:N0}", numberOfMessages);
            Console.WriteLine("# Messages           : {0:N0}", numberOfBatches * numberOfMessages);
            Console.WriteLine();

            var eventHubProducerClient =
                new EventHubProducerClient(
                    _configuration["EventHub_PrimaryConnectionString"],
                    _configuration["EventHub_Name"]);

            var message = await File.ReadAllTextAsync("sample.json");

            for (var batchIndex = 0; batchIndex < numberOfBatches; batchIndex++)
            {
                var partitionId = 0;

                var createBatchOptions =
                    new CreateBatchOptions()
                    {
                        PartitionId = partitionId.ToString()
                    };

                var eventDataBatch =
                    await eventHubProducerClient.CreateBatchAsync(createBatchOptions);

                for (var messageIndex = 0; messageIndex < numberOfMessages; messageIndex++)
                {
                    var eventData =
                        new EventData(
                            Encoding.UTF8.GetBytes(message));

                    eventDataBatch.TryAdd(eventData);
                }

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Sending batch {0:N0} with {1:N0} message(s)...", batchIndex + 1, numberOfMessages);

                await eventHubProducerClient.SendAsync(
                    eventDataBatch);

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Batch sent.");
                Console.WriteLine();

                partitionId++;

                if (partitionId > 1) _ = 0;
            }

            Console.ResetColor();
            Console.Write("Press any key to exit...");
        }
    }
}
