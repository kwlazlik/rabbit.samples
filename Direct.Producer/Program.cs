﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using RabbitMQ.Client;

namespace RabbitSamples.HelloWorld.Producer
{
   internal static class Program
   {
      public static async Task Main()
      {
         var factory = new ConnectionFactory
         {
            UserName = ConnectionFactory.DefaultUser,
            Password = ConnectionFactory.DefaultPass,
            VirtualHost = ConnectionFactory.DefaultVHost,
            HostName = "localhost",
            Port = AmqpTcpEndpoint.UseDefaultPort
         };

         using IConnection connection = factory.CreateConnection();
         using IModel channel = connection.CreateModel();

         channel.QueueDeclare(queue: "sample-direct-queue", durable: false, exclusive: false, autoDelete: false, arguments: null);

         while (true)
         {
            string message = PickMessage();
            byte[] body = Encoding.UTF8.GetBytes(message);

            channel.BasicPublish(exchange: "", routingKey: "sample-direct-queue", basicProperties: null, body: body);

            Console.WriteLine($"--- Message sent: {message}");

            await Task.Delay(2000);
         }
      }

      private static readonly Random Random = new Random();

      private static T Pick<T>(this IReadOnlyList<T> list) => list[Random.Next(list.Count)];

      private static string PickMessage()
      {
         string[] colors =
         {
            "red",
            "green",
            "yellow"
         };

         string[] taste =
         {
            "sweet",
            "spicy"
         };

         string[] vegetables =
         {
            "carrot",
            "tomato",
            "pepper"
         };

         return $"{colors.Pick()} {taste.Pick()} {vegetables.Pick()} {DateTime.Now:HH:mm:ss.fff}";
      }
   }
}