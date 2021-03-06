﻿using System;
using System.Configuration;
using Microsoft.ServiceBus.Messaging;

namespace EventHubsReceiver
{
    class Program
    {
        static void Main(string[] args)
        {
            string eventHubConnectionString = ConfigurationManager.AppSettings["eventHubReceiveRuleConnectionString"];
            string eventHubName = ConfigurationManager.AppSettings["eventHubName"];
            string storageAccountName = ConfigurationManager.AppSettings["storageAccountName"];
            string storageAccountKey = ConfigurationManager.AppSettings["storageAccountKey"];
            string storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", storageAccountName, storageAccountKey);

            string eventProcessorHostName = Guid.NewGuid().ToString();
            EventProcessorHost eventProcessorHost = new EventProcessorHost(eventProcessorHostName, eventHubName, EventHubConsumerGroup.DefaultGroupName, eventHubConnectionString, storageConnectionString);

            Console.WriteLine("Registering EventProcessor...");
            var options = new EventProcessorOptions();
            options.ExceptionReceived += (sender, e) => { Console.WriteLine(e.Exception); };
            eventProcessorHost.RegisterEventProcessorAsync<SimpleEventProcessor>(options).Wait();

            Console.WriteLine("Receiving. Press enter key to stop worker.");
            Console.ReadLine();
            eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }
    }
}
