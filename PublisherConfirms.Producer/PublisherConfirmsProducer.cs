using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using RabbitMQ.Client;

namespace RabbitSamples.PublisherConfirms.Producer
{
   internal static class PublisherConfirmsProducer
   {
      private const int MessageCount = 5000;
      private const double MessageSizeMb = 1;

      public static void Main()
      {
         PublishAndConfirmSync();
         PublishAndConfirmAsync();
      }

      private static void PublishAndConfirmSync()
      {
         using IConnection connection = new ConnectionFactory().CreateConnection();
         using IModel channel = connection.CreateModel();

         string queueName = channel.QueueDeclare().QueueName;
         channel.ConfirmSelect();

         byte[] body = GetMessageBody(MessageSizeMb);

         var timer = Stopwatch.StartNew();

         for (var i = 0; i < MessageCount; i++)
         {
            channel.BasicPublish("", queueName, null, body);
         }

         bool noNack = channel.WaitForConfirms(new TimeSpan(0, 0, 0, 0, 10), out bool timedOut);
         //channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 0, 0, 10));

         timer.Stop();

         Console.WriteLine($"--- Published {MessageCount:N0} messages individually in {timer.ElapsedMilliseconds:N0} ms");
         Console.Read();
      }

      private static void PublishAndConfirmAsync()
      {
         using IConnection connection = new ConnectionFactory().CreateConnection();
         using IModel channel = connection.CreateModel();

         string queueName = channel.QueueDeclare().QueueName;
         channel.ConfirmSelect();

         var outstandingConfirms = new ConcurrentDictionary<ulong, string>();

         void CleanOutstandingConfirms(ulong sequenceNumber, bool multiple)
         {
            if (multiple)
            {
               foreach (KeyValuePair<ulong, string> kv in outstandingConfirms.Where(k => k.Key <= sequenceNumber))
               {
                  outstandingConfirms.TryRemove(kv.Key, out _);
               }
            }
            else
            {
               outstandingConfirms.TryRemove(sequenceNumber, out _);
            }
         }

         channel.BasicAcks += (sender, message) => CleanOutstandingConfirms(message.DeliveryTag, message.Multiple);

         channel.BasicNacks += (sender, message) =>
         {
            outstandingConfirms.TryGetValue(message.DeliveryTag, out string body);
            Console.WriteLine($"Message with body {body} has been nack-ed. Sequence number: {message.DeliveryTag}, multiple: {message.Multiple}");
            CleanOutstandingConfirms(message.DeliveryTag, message.Multiple);
         };

         byte[] messageBody = GetMessageBody(MessageSizeMb);

         var timer = new Stopwatch();
         timer.Start();

         for (var i = 0; i < MessageCount; i++)
         {
            outstandingConfirms.TryAdd(channel.NextPublishSeqNo, i.ToString());
            channel.BasicPublish("", queueName, null, messageBody);
         }

         if (!WaitUntil(1, () => outstandingConfirms.IsEmpty))
         {
            throw new Exception("All messages could not be confirmed in 60 seconds");
         }

         timer.Stop();

         Console.WriteLine($"--- Published {MessageCount:N0} messages and handled confirm asynchronously {timer.ElapsedMilliseconds:N0} ms");
         Console.Read();
      }

      private static byte[] GetMessageBody(double megaBytes)
      {
         byte[] body = new byte[(int)(megaBytes * 1024 * 1024)];
         new Random().NextBytes(body);
         return body;
      }

      private static bool WaitUntil(int numberOfSeconds, Func<bool> condition)
      {
         var waited = 0;

         while (!condition() && waited < numberOfSeconds * 1000)
         {
            Thread.Sleep(100);
            waited += 100;
         }

         return condition();
      }
   }
}