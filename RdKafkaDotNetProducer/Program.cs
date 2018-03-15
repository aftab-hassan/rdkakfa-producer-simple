using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace RdKafkaDotNetProducer
{
    class Program
    {
        public static void Main(string[] args)
        {
            //string brokerList = "40.118.249.252:9092";
            string brokerList = "138.91.140.244:9092";
            //string brokerList = "localhost";
            string topicName = "testLikeWin";

            using (Producer producer = new Producer(brokerList))
            using (Topic topic = producer.Topic(topicName))
            {
                Console.WriteLine($"{producer.Name} producing on {topic.Name}. q to exit.");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    byte[] data = Encoding.UTF8.GetBytes(text);
                    Task<DeliveryReport> deliveryReport = topic.Produce(data);
                    var unused = deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
            }
        }
    }
}
