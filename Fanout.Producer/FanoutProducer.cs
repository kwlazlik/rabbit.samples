using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitSamples.Fanout.Producer
{
   internal static class FanoutProducer
   {
      public static async Task Main(string[] args)
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

         channel.ExchangeDeclare(exchange: "sample-fanout-exchange", type: ExchangeType.Fanout);

         while (true)
         {
            string messageText = PickMessage();
            byte[] body = Encoding.UTF8.GetBytes(messageText);

            channel.BasicPublish(exchange: "sample-fanout-exchange", routingKey: "", basicProperties: null, body: body);

            Console.WriteLine($"--- Message sent: {messageText}");

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
