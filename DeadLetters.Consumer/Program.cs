﻿using System;
using System.Collections.Generic;
using System.Text;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// ReSharper disable ArgumentsStyleOther
// ReSharper disable AccessToDisposedClosure
// ReSharper disable ArgumentsStyleNamedExpression
// ReSharper disable ArgumentsStyleLiteral
// ReSharper disable ArgumentsStyleStringLiteral

namespace RabbitSamples.DeadLetters.Consumer
{
   internal static class Program
   {
      private static readonly Random Random = new Random();

      public static void Main()
      {
         ConnectionFactory factory = new ConnectionFactory();

         using IConnection connection = factory.CreateConnection();
         using IModel channel = connection.CreateModel();

         channel.ExchangeDeclare("task-exchange", type: "direct", durable: false, autoDelete: false,
            arguments: new Dictionary<string, object>
            {
               { "alternate-exchange", "dl-exchange" }
            });

         channel.QueueDeclare("task-queue", durable: false, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object>
            {
               { "x-dead-letter-exchange", "dl-exchange" }
            });

         channel.QueueBind(queue: "task-queue", exchange: "task-exchange", routingKey: "task-rk");

         channel.ExchangeDeclare("dl-exchange", type: "fanout", durable: false, autoDelete: false, arguments: null);
         channel.QueueDeclare("dl-queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
         channel.QueueBind(queue: "dl-queue", exchange: "dl-exchange", routingKey: "");

         EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
         consumer.Received += (model, ea) =>
         {
            byte[] body = ea.Body;
            string message = Encoding.UTF8.GetString(body);

            if (Random.Next() % 10 == 0)
            {
               channel.BasicNack(ea.DeliveryTag, false, false);
               Console.WriteLine($"-- Message rejected: {message} {ea.RoutingKey}");
            }
            else
            {
               channel.BasicAck(ea.DeliveryTag, false);
               Console.WriteLine($"-- Message received: {message} {ea.RoutingKey}");
            }
         };

         channel.BasicConsume(queue: "task-queue", autoAck: false, consumer: consumer);

         EventingBasicConsumer dlConsumer = new EventingBasicConsumer(channel);
         dlConsumer.Received += (model, ea) =>
         {
            byte[] body = ea.Body;
            string message = Encoding.UTF8.GetString(body);

            Console.WriteLine($"-- Dead Letter Message received: {message} {ea.RoutingKey}");
         };

         channel.BasicConsume(queue: "dl-queue", autoAck: false, consumer: dlConsumer);

         Console.ReadLine();
      }
   }
}