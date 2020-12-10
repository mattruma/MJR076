using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace MJR076
{
    class Program
    {     
        protected static DateTime _startedAt;
        protected static DateTime _stopAt;

        /// <param name="quantity">The number of messages to send.</param>
        /// <param name="interval">The interval, expressed in seconds, at which to send messages.</param>
        /// <param name="duration">The duration, expressed in seconds, after which to stop sending messages.</param>
        /// <param name="fileName">The name of the file to use as a template for the message.</param>
        /// <param name="eventHubConnectionString">The event hub connection string.</param>
        /// <param name="eventHubName">The name of the event hub to send messages to and if not included in the eventHubConnectionString.</param>
        public static async Task Main(
            string eventHubConnectionString,
            string eventHubName,
            int quantity = 10,
            int interval = 1,
            int duration = 60,
            string fileName = "sample.json")
        {
            Console.ResetColor();
            Console.WriteLine("Event Hub Spammer");
            Console.WriteLine();
            Console.WriteLine("Sending {0:N0} message(s) every {1:N0} second(s) for {2:N0} second(s)", quantity, interval, duration);
            Console.WriteLine();

            var eventHubConnectionStringParts = 
                eventHubConnectionString.Split(";");

            if (eventHubConnectionStringParts.Length == 4)
            {
                eventHubName =
                    eventHubConnectionStringParts[3].Split("=")[1];
                eventHubConnectionString =
                    eventHubConnectionStringParts[0] + ";" + eventHubConnectionStringParts[1] + ";" + eventHubConnectionStringParts[2];
            }

            Console.WriteLine($"File Name                   : {fileName}");
            Console.WriteLine($"Event Hub Connection String : {eventHubConnectionString}");
            Console.WriteLine($"Event Hub Name              : {eventHubName}");
            Console.WriteLine();

            var eventHubProducerClient =
                new EventHubProducerClient(
                    eventHubConnectionString,
                    eventHubName);

            var message = await File.ReadAllTextAsync(fileName);

            var timer = new Timer(interval * 1000);

            timer.Elapsed += async (sender, e) => await ExecuteAsync(eventHubProducerClient, quantity, message);

            _startedAt = DateTime.Now;
            _stopAt = _startedAt.AddSeconds(duration);

            timer.Start();

            Console.WriteLine("{0}: Started", _startedAt);

            while (true)
            {
                if (DateTime.Now > _stopAt)
                {
                    timer.Stop();
                    break;
                }
            }

            Console.WriteLine("{0}: Stopped", DateTime.Now);
            Console.ReadKey();
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
            var stopWatch = new Stopwatch();

            stopWatch.Start();

            var tasks = new List<Task>();

            for (var messageIndex = 0; messageIndex < quantity; messageIndex++)
            {
                var messageToSend = message;

                messageToSend = messageToSend.Replace("{{CURRENTDATETIME}}", DateTime.UtcNow.ToString());

                tasks.Add(Task.Run(async () => await SendMessageAsync(eventHubProducerClient, message)));
            }

            var startedAt = DateTime.Now;

            await Task.WhenAll(tasks);

            var ts = stopWatch.Elapsed;

            var elapsedTime =
                (ts.Hours * 60 * 60 * 1000) + (ts.Minutes * 60 * 1000) + (ts.Seconds * 1000) + ts.Milliseconds;

            var elapsedTimeString = $"{ts.Milliseconds} millisecond(s)";

            if (elapsedTime > 1000) elapsedTimeString = $"{ts.Seconds} second(s)";

            Console.WriteLine("{0}: Sent {1:N0} message(s), took {2}.", DateTime.Now, quantity, elapsedTimeString);
        }
    }
}
