using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitSamples.Headers.Producer
{
   internal static class HeadersProducer
   {
      public static async Task Main()
      {
         var factory = new ConnectionFactory();

         using var connection = factory.CreateConnection();
         using var channel = connection.CreateModel();

         channel.ExchangeDeclare("sample-headers-exchange", ExchangeType.Headers, false, false, null);

         while (true)
         {
            (string fruit, string color) = PickFruitAndColor();

            var messageText = $"{color} {fruit}";
            var body = Encoding.UTF8.GetBytes(messageText);

            IBasicProperties properties = channel.CreateBasicProperties();
            properties.Headers = new Dictionary<string, object>
            {
               {"fruit", fruit},
               {"color", color}
            };

            channel.BasicPublish("sample-headers-exchange", "", properties, body);

            Console.WriteLine("--- Message sent: {0}", messageText);

            await Task.Delay(3000);
         }
      }

      private static readonly Random Random = new Random();

      private static T Pick<T>(this IReadOnlyList<T> list) => list[Random.Next(list.Count)];

      private static (string, string) PickFruitAndColor()
      {
         string[] fruits =
         {
            "apple",
            "cherry",
            "strawberry"
         };
         string[] colors =
         {
            "red",
            "green",
            "yellow"
         };

         return (fruits.Pick(), colors.Pick());
      }
   }
}