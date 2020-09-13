﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Fanout.Producer
{
   internal static class Program
   {
      public static async Task Main(string[] args)
      {
         var factory = new ConnectionFactory();

         using IConnection connection = factory.CreateConnection();
         using IModel channel = connection.CreateModel();

         channel.ExchangeDeclare(exchange: "sample-fanout-exchange", type: ExchangeType.Fanout);

         while (true)
         {
            string message = PickMessage();
            var body = Encoding.UTF8.GetBytes(message);

            channel.BasicPublish(exchange: "sample-fanout-exchange", routingKey: "", basicProperties: null, body: body);

            Console.WriteLine($"--- Message sent: {message}");

            await Task.Delay(Random.Next(1500, 3000));
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