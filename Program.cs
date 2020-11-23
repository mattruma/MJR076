using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
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
        protected static DateTime _startedAt;
        protected static DateTime _stopAt;

        /// <param name="quantity">The number of messages to send.</param>
        /// <param name="interval">The interval, expressed in seconds, at which to send messages.</param>
        /// <param name="duration">The duration, expressed in seconds, after which to stop sending messages.</param>
        public static async Task Main(
            int quantity = 10,
            int interval = 1,
            int duration = 60)
        {
            _configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
                .Build();

            Console.ResetColor();
            Console.WriteLine("Event Hub Spammer");
            Console.WriteLine();
            Console.WriteLine("Quantity     : {0:N0}", quantity);
            Console.WriteLine("Interval     : {0:N0}", interval);
            Console.WriteLine("Duration     : {0:N0}", duration);
            Console.WriteLine();

            var eventHubProducerClient =
                new EventHubProducerClient(
                    _configuration["EventHub_PrimaryConnectionString"],
                    _configuration["EventHub_Name"]);

            var message = await File.ReadAllTextAsync("sample.json");

            var timer = new System.Timers.Timer(interval * 1000);

            timer.Elapsed += async (sender, e) => await ExecuteAsync(eventHubProducerClient, quantity, message);

            _startedAt = DateTime.Now;
            _stopAt = _startedAt.AddSeconds(duration);

            timer.Start();

            Console.WriteLine("{0}: Started", _startedAt);

            while (DateTime.Now < _stopAt)
            {

            }

            Console.WriteLine("{0}: Stopped", DateTime.Now);
        }

        public static async Task SendMessageAsync(
            EventHubProducerClient eventHubProducerClient,
            string message)
        {
            var eventDataBatch =
                 await eventHubProducerClient.CreateBatchAsync();

            var eventData =
                new EventData(
                    Encoding.UTF8.GetBytes(message));

            eventDataBatch.TryAdd(eventData);

            await eventHubProducerClient.SendAsync(eventDataBatch);
        }

        public static async Task ExecuteAsync(
            EventHubProducerClient eventHubProducerClient,
            int quantity,
            string message)
        {
            Console.WriteLine("{0}: Sending {1} message(s)...", DateTime.Now, quantity);

            var tasks = new List<Task>();

            for (var messageIndex = 0; messageIndex < quantity; messageIndex++)
            {
                tasks.Add(Task.Run(async () => await SendMessageAsync(eventHubProducerClient, message)));
            }

            await Task.WhenAll(tasks);
        }
    }
}
