using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace EventHubsMonitor
{
    class Program
    {
        static EventHubClient _client;

        static void Main(string[] args)
        {
            string eventHubConnectionString = ConfigurationManager.AppSettings["eventHubReceiveRuleConnectionString"];
            string eventHubName = ConfigurationManager.AppSettings["eventHubName"];

            _client = EventHubClient.CreateFromConnectionString(eventHubConnectionString, eventHubName);

            MonitorEventHub().Wait();
        }

        static async Task MonitorEventHub()
        {
            EventHubRuntimeInformation runtimeInfo = await _client.GetRuntimeInformationAsync();
            Console.WriteLine($"Partition count: {runtimeInfo.PartitionCount}");

            long[] sequenceNumbers = new long[runtimeInfo.PartitionCount];

            for (;;)
            {
                var tasks = new Task<PartitionRuntimeInformation>[runtimeInfo.PartitionCount];

                for (int i = 0; i < runtimeInfo.PartitionCount; i++)
                {
                    tasks[i] = _client.GetPartitionRuntimeInformationAsync(i.ToString());
                }

                // Wait for all of them
                await Task.WhenAll(tasks);

                for (int i = 0; i < runtimeInfo.PartitionCount; i++)
                {
                    PartitionRuntimeInformation part = tasks[i].Result;
                    Console.WriteLine($"BeginSequenceNumber: {part.BeginSequenceNumber}, LastEnqueuedOffset: {part.LastEnqueuedOffset}, LastEnqueuedSequenceNumber: {part.LastEnqueuedSequenceNumber}, LastEnqueuedTime: {part.LastEnqueuedTimeUtc}");

                    if (sequenceNumbers[i] != 0)
                    {
                        if (part.LastEnqueuedSequenceNumber != sequenceNumbers[i])
                        {
                            Console.WriteLine($"Sequence when up by {part.LastEnqueuedSequenceNumber - sequenceNumbers[i]} for partition {i}!");
                        }
                    }

                    sequenceNumbers[i] = part.LastEnqueuedSequenceNumber;
                }

                System.Threading.Thread.Sleep(5000);
            }
        }
    }
}
