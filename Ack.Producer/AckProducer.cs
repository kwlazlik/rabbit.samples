using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using RabbitMQ.Client;

namespace RabbitSamples.Ack.Producer
{
   internal static class AckProducer
   {
      public static async Task Main()
      {
         var factory = new ConnectionFactory();

         using IConnection connection = factory.CreateConnection();
         using IModel channel = connection.CreateModel();

         channel.QueueDeclare(queue: "sample-durable-queue", durable: true, exclusive: false, autoDelete: false, arguments: null);

         IBasicProperties properties = channel.CreateBasicProperties();
         properties.Persistent = true;

         while (true)
         {
            string messageText = PickMessage();
            byte[] body = Encoding.UTF8.GetBytes(s: messageText);

            channel.BasicPublish(exchange: "", routingKey: "sample-durable-queue", basicProperties: properties, body: body);

            Console.WriteLine("--- Message sent: {0}", messageText);

            await Task.Delay(3000);
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