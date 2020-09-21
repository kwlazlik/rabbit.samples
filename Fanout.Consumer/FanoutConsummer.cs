using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitSamples.Fanout.Consumer
{
   internal static class FanoutConsumer
   {
      public static void Main()
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

         string queueName = channel.QueueDeclare().QueueName;
         channel.QueueBind(queue: queueName, exchange: "sample-fanout-exchange", routingKey: "", arguments: null);

         var consumer = new EventingBasicConsumer(channel);
         consumer.Received += (model, message) =>
         {
            byte[] body = message.Body.ToArray();
            string messageText = Encoding.UTF8.GetString(body);

            Console.WriteLine("--- Message received: {0}", messageText);
         };

         channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

         Console.WriteLine("--- Waiting for messages ...");
         Console.Read();
      }
   }
}
