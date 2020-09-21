using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using RabbitMQ.Client;

namespace RabbitSamples.DeadLetters.Producer
{
   internal static class DeadLettersProducer
   {
      public static async Task Main()
      {
         var factory = new ConnectionFactory();

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

         channel.QueueBind(queue: "task-queue", exchange: "task-exchange", routingKey: "valid-rk");

         channel.ExchangeDeclare("dl-exchange", type: ExchangeType.Fanout, durable: false, autoDelete: false, arguments: null);

         channel.QueueDeclare("dl-queue", durable: false, exclusive: false, autoDelete: false, arguments: null);

         channel.QueueBind(queue: "dl-queue", exchange: "dl-exchange", routingKey: "");

         while (true)
         {
            (string messageText, string key) = PickMessageAndKey();
            byte[] body = Encoding.UTF8.GetBytes(messageText);

            channel.BasicPublish(exchange: "task-exchange", routingKey: key, basicProperties: null, body: body);

            Console.WriteLine("--- Message sent: {0}", messageText);

            await Task.Delay(3000);
         }
      }

      private static readonly Random Random = new Random();

      private static T Pick<T>(this IReadOnlyList<T> list) => list[Random.Next(list.Count)];

      private static (string, string) PickMessageAndKey()
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

         string key = Random.Next() % 4 == 0 ? "invalid-key" : "valid-key";
         string messageText = $"{colors.Pick()} {taste.Pick()} {vegetables.Pick()} {key} {DateTime.Now:HH:mm:ss.fff}";

         return (messageText, key);
      }
   }
}