using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitSamples.Headers.Consumer
{
   internal static class HeadersConsumer
   {
      public static void Main()
      {
         var factory = new ConnectionFactory();

         using IConnection connection = factory.CreateConnection();
         using IModel channel = connection.CreateModel();

         channel.ExchangeDeclare(exchange: "sample-headers-exchange", type: ExchangeType.Headers, durable: false, autoDelete: false, arguments: null);

         string queueName = channel.QueueDeclare().QueueName;

         channel.QueueBind(queue: queueName, exchange: "sample-headers-exchange", routingKey: "", arguments: new Dictionary<string, object>
         {
            { "x-match", "any" },
            { "fruit", "apple" },
            { "color", "red" }
         });

         Console.WriteLine("--- Waiting for messages: fruit=apple or color=red");

         EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
         consumer.Received += (model, message) =>
         {
            ReadOnlyMemory<byte> body = message.Body;
            string messageText = Encoding.UTF8.GetString(body.Span);

            Console.WriteLine($"--- Message received: {messageText}");
         };

         channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

         Console.WriteLine("--- Waiting for messages ...");
         Console.Read();
      }
   }
}