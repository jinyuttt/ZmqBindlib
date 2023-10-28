using MQBindlib;
using static Confluent.Kafka.ConfigPropertyNames;
using System.Threading;
using NetMQ.Sockets;
using NetMQ;
using System.Net.Sockets;
using System.Net;
using System.Net.NetworkInformation;

namespace ConsoleApp1
{
    internal class Program
    {
        static string localaddes = "tcp://192.168.237.55:6666";
       static EhoServer server= new EhoServer();
        static string address = "";
        private static string sr;

        static void Read()
        {
            using (StreamReader rd = new StreamReader(Directory.GetCurrentDirectory() + "\\config.txt"))
            {
               address=rd.ReadToEnd();
            }
        }
        static void Main(string[] args)
        {
            //Read();
            //  TestGroup();
            //  DDProxy();
            //  Test();
            // kfkasub();
            //kfkaPub();
            //  Rep();
            //  Req();
            // DDProxy();
           //  TestClusterPUb();
         // TestClusterSub();
           
           //  Sub();
             TestSub();
            pub();
            // TestPub();
            //  TestSub();
           // TestEhoServer();
            Console.WriteLine("Hello, World!");
            // TestCluster();
            // TestIP();
            // recvice();
           // Req();
            Console.ReadLine();
        }
        static void TestSub()
        {
            ZmqSubscriberGroup zmqSubscriber=new ZmqSubscriberGroup();
            zmqSubscriber.Address = "tcp://192.168.237.55:6666";
           // zmqSubscriber.IsDDS= true;//高可用
            zmqSubscriber.DataOffset = DataModel.Earliest;
           // zmqSubscriber.Indenty = "test";//订阅在不同分组
            zmqSubscriber.Subscribe("A");
            zmqSubscriber.StringReceived += ZmqSubscriber_StringReceived;
        }

        private static void ZmqSubscriber_StringReceived(string arg1, string arg2)
        {
           Console.WriteLine(arg2 + " " + arg1);
        }

        static void TestClusterSub()
        {
            ZmqPullProxy.PubAddress = "tcp://192.168.237.55:6666";
            ZmqPullProxy.SubAddress = "tcp://192.168.237.55:6667";
            //ZmqDDSProxy.IsCluster = true;//高可用
            // ZmqDDSProxy.IsStorage = true;
            bool isret = false;
            do
            {
                isret = ZmqPullProxy.Start(); //注意方法，启动和另外发布订阅方法不同
                Thread.Sleep(1000);
            }
            while (!isret);
        }
        static void TestClusterPUb()
        {
            ZmqDDSProxy.PubAddress = "tcp://192.168.237.55:6666";
            ZmqDDSProxy.SubAddress = "tcp://192.168.237.55:6667";
            // ZmqDDSProxy.IsCluster=true;
            bool isRet = false;
            do
            {
                isRet = ZmqDDSProxy.Start();
                Thread.Sleep(1000);
            } while (!isRet);
        }

        static void TestEhoServer()
        {


            EhoServer eho = new EhoServer();
          //  eho.IsCluster = true;
            eho.DealerAddress = "inproc://server";
            eho.RouterAddress = "tcp://127.0.0.1:5550";
            string addr = Util.GetLocalAllIP()[0];
          //  Console.WriteLine("bind :" + addr);
            eho.RouterAddress = localaddes;
            eho.StringReceived += EhoServer_StringReceived;
            do
            {
                try
                {
                    if (eho.Start())
                    {
                        break;
                    }

                }
                catch (Exception ex)
                {
                    // ErrorCode AddressNotAvailable NetMQ.ErrorCode
                    if (ex.HResult != -2146233088)
                    {
                        break;
                    }
                   

                }
                Thread.Sleep(1000);
            } while (true);

            //Console.Title = "5560";
            //EhoServer ehoServer = new EhoServer();
            //ehoServer.IsCluster = true;
            //ehoServer.StringReceived += EhoServer_StringReceived;
            //ehoServer.Start();

            //Thread thread = new Thread(p =>
            //{
            //    Thread.Sleep(10000);
            //    eho.Close();

            //    Console.WriteLine("exit!");
            //});
            //thread.Start();

            // Req();
        }

        private static void EhoServer_StringReceived(object? sender, RspSocket<string> e)
        {
            Console.WriteLine("Server:"+e.Message);
            if (e.Message == "hi")
            {
                //  Thread.Sleep(4000);
                e.Response("jinyu");
                return;
            }
            e.Response("word");
        }

        static void Req()
        {
            Thread thread = new Thread(Req1);
            thread.Name = "REq1";
            thread.Start();

            Thread thread1 = new Thread(Req2);
            thread1.Name = "REq2";
            thread1.Start();


        }
        static void Req1()
        {
           
            ZmqRequest request = new ZmqRequest();
            request.RemoteAddress = localaddes;
            request.Client = "A";
            int num = 0;
            while (true)
            {
                   Thread.Sleep(1000);
                try
                {
                    string msg = request.Request("hi");
                    //var p= request.Request<Person,Person>(new Person { Name = "jin", Description = "请求", Id = num++, Title = "rr" });
                    //  Console.WriteLine(p.Description+p.Name);
                    Console.WriteLine(msg);
                }
                catch (Exception e) { }
            }

        }
        static void Req2()
        {



            ZmqRequest request = new ZmqRequest();
            request.RemoteAddress = localaddes;
            request.Client = "B";
            int num = 0;
            while (true)
            {
                  Thread.Sleep(1000);
                try
                {
                    //  Person p = request.Request<Person, Person>(new Person { Name = "yu", Description = "请求", Id = num++, Title = "ss" });
                    string msg = request.Request("hello");
                    Console.WriteLine(msg);

                }
                catch(Exception e) { }
                // request.RequestCluster();
               
                //Console.WriteLine(p.Description + p.Name);
            }

        }



        static void Rep()
        {
            //ZmqResponse rep = new ZmqResponse();
            //rep.LocalAddress = localaddes;
            //rep.Start();
            //int num = 0;
            ////rep.ByteReceived += (sender, e) =>
            ////{
            ////    Console.WriteLine(System.Text.Encoding.Default.GetString(e));
            ////    rep.Response("word"+num++);
            ////};
            //rep.StringReceived += (sender, e) =>
            //{
            //    Console.WriteLine(e);
            //    if (e == "hi")
            //    {
            //        Thread.Sleep(1000);
            //    }
            //    rep.Response("word" + num++);
            //};

            server =new EhoServer();
            server.IsCluster = true;
          
           // server.IsEmptyReturn = true;
           // server.RouterAddress = "tcp://127.0.0.1:66666";//服务地址，请求的远端地址
            //  server.ByteReceived += Server_ByteReceived;
           server.StringReceived += Server_StringReceived1; 
            server.Start();
           
           
        }

        private static  void recvice()
        {
            while (true)
            {
                var ss = server.GetMsg<Person>();
                ss.Message.Description = "回复"+ss.Message.Id;
                ss.Response(ss.Message);
            }
        }
        private static void Server_StringReceived1(object? sender, RspSocket<string> e)
        {
            Console.WriteLine(e.Message);
            if (e.Message == "hi")
            {
              //  Thread.Sleep(4000);
                e.Response("jinyu");
                return;
            }
            e.Response("word");
        }

        private static void Server_StringReceived(object? sender, string e)
        {
            Console.WriteLine(e);
           // if (e == "hi")
            {
                Thread.Sleep(4000);
            }
           // server.Response("word");
        }

        private static void Server_ByteReceived(object? sender, byte[] e)
        {
            Console.WriteLine(System.Text.Encoding.Default.GetString(e));
        }

        static void Sub()
        {
            ZmqSubscriber sub = new ZmqSubscriber();
            sub.Address = new string[] { "tcp://192.168.237.55:6666" };
           // sub.IsDDS = true;//高可用启动
            sub.Subscribe("");
           // sub.ByteReceived += Sub_ByteReceived;
            sub.StringReceived += Sub_StringReceived;
        }

        private static void Sub_StringReceived(string arg1, string arg2)
        {
            Console.WriteLine(arg2);
        }

        private static void Sub_ByteReceived(string arg1, byte[] arg2)
        {
            Console.WriteLine(System.Text.Encoding.Default.GetString(arg2));
        }

        static void pub()
        {
            ZmqPublisher pub = new ZmqPublisher();
            pub.Address = "tcp://192.168.237.55:6667";
            pub.IsProxy = true; //是否使用中间代理
           // pub.IsDDS = true;//高可用启动
            int num = 0;
            while (true)
            {
                Thread.Sleep(1000);
                try
                {
                    pub.Publish("A", "ssss" + num++);
                }catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }

            }

        }
        static void DDProxy()
        {
            string tmp = string.Format("tcp://{0}:", address);
            ZmqDDSProxy.PubAddress =tmp+"1234";
            ZmqDDSProxy.SubAddress = tmp+"5678";
            ZmqDDSProxy.Start();
        }
       


        static void  kfkaPub()
        {
            Thread thread = new Thread(p =>
            {
               
                KafkaPublisher kafkaPublisher = new KafkaPublisher();
                int num = 0;
                while (true)
                {
                    Thread.Sleep(1000);
                    kafkaPublisher.Push("A", "SSSSS"+num++);
                }
            });
            thread.Start();
        }
        static void kfkasub()
        {
            Thread thread = new Thread(p =>
            {
               
                KafkaSubscriber  kafkaSubscriber = new KafkaSubscriber();
                kafkaSubscriber.Subscriber("A");
                kafkaSubscriber.Consume(p =>
                {
                    if(p==null)
                    {
                        return;
                    }
                    Console.WriteLine(string.Format("Received message at {0}:{1}", p.Topic, p.Value));
                
                });
            });
            thread.Start();
        }

        static void Test()
        {
            Thread sub = new Thread(p =>
            {
                string topic = "" /* ... */; // one of "TopicA" or "TopicB"
                using (var subSocket = new SubscriberSocket(">tcp://127.0.0.1:1234"))
                {
                    subSocket.Options.ReceiveHighWatermark = 1000;
                    subSocket.Subscribe(topic);
                    Console.WriteLine("Subscriber socket connecting...");
                    while (true)
                    {
                        string messageTopicReceived = subSocket.ReceiveFrameString();
                        string messageReceived = subSocket.ReceiveFrameString();
                        Console.WriteLine(messageReceived);
                    }
                }
            });
           sub.Start();

            Thread pub = new Thread(p =>
            {
                using (var pubSocket = new PublisherSocket(">tcp://127.0.0.1:5678"))
                {
                    Console.WriteLine("Publisher socket connecting...");
                    pubSocket.Options.SendHighWatermark = 1000;
                    var rand = new Random(50);

                    while (true)
                    {
                        var randomizedTopic = rand.NextDouble();
                        if (randomizedTopic > 0.5)
                        {
                            var msg = "TopicA msg-" + randomizedTopic;
                           // Console.WriteLine("Sending message : {0}", msg);
                            pubSocket.SendMoreFrame("A").SendFrame(msg);
                        }
                        else
                        {
                            var msg = "TopicB msg-" + randomizedTopic;
                         //   Console.WriteLine("Sending message : {0}", msg);
                            pubSocket.SendMoreFrame("TopicB").SendFrame(msg);
                        }
                    }
                }
            });
            pub.Start();

          
           // proxy.Start();
        }

        static void TestSub1()
        {
            ZmqBus zmqBus=new ZmqBus();
            zmqBus.Subscribe("TopicB");
            zmqBus.StringReceived += ZmqBus_StringReceived;

        }
        static void TestPub()
        {
            Thread pub = new Thread(p =>
            {
                ZmqBus zmqBus = new ZmqBus();
                while (true)
                {
                    Thread.Sleep(1000);
                    zmqBus.Publish("TopicB", "sssss");
                    zmqBus.Publish("TopicA", "rrrr");
                }
              
            });
            pub.Start();
        }

        private static void ZmqBus_StringReceived(string arg1, string arg2)
        {
            Console.WriteLine(arg2);
        }

        static void TestGroup()
        {
            ZmqPullProxy.SubAddress = "tcp://127.0.0.1:5555";
            ZmqPullProxy.PubAddress= "tcp://127.0.0.1:6666";
            ZmqPullProxy.Start();
            Thread.Sleep(2000);
            Thread thread = new Thread(p => {
               // Thread.Sleep(20000);
                ZmqSubscriberGroup zmqSubscriberGroup=new ZmqSubscriberGroup();
                zmqSubscriberGroup.Address= "tcp://127.0.0.1:6666";
                zmqSubscriberGroup.Subscribe("TopicA");
                zmqSubscriberGroup.StringReceived += ZmqSubscriberGroup_StringReceived;

                ZmqSubscriberGroup zmqSubscriberGroup1 = new ZmqSubscriberGroup();
                zmqSubscriberGroup1.Address = "tcp://127.0.0.1:6666";
              //  zmqSubscriberGroup1.Indenty = "Q";
                zmqSubscriberGroup1.Subscribe("TopicA");
                zmqSubscriberGroup1.StringReceived += ZmqSubscriberGroup_StringReceived1;
                Thread.Sleep(30000);
                zmqSubscriberGroup1.Close();
            });
            thread.Start();

            Thread thread1 = new Thread(p => {
               
                ZmqPublisher zmqPublisher = new ZmqPublisher();
                zmqPublisher.IsProxy = true;
                zmqPublisher.Address = "tcp://127.0.0.1:5555";
                int num = 0;
                while(true)
                {
                   Thread.Sleep(10);
                    zmqPublisher.Publish("TopicA", "AAAAA"+num++);
                }
            });
            thread1.Start();

        }

        private static void ZmqSubscriberGroup_StringReceived1(string arg1, string arg2)
        {
            Console.WriteLine("B:"+arg2);
        }

        private static void ZmqSubscriberGroup_StringReceived(string arg1, string arg2)
        {
            Console.WriteLine("A:"+arg2);
        }

        static void TestIP()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    Console.WriteLine("IP Address = " + ip.ToString());
                }
            }
            Console.WriteLine("方法1");

            string localIP = string.Empty;
            using (Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, 0))
            {
                socket.Connect("8.8.8.8", 65530);
                IPEndPoint endPoint = socket.LocalEndPoint as IPEndPoint;
                localIP = endPoint.Address.ToString();
            }
            Console.WriteLine("IP Address = " + localIP);
            Console.WriteLine("方法2");

            string output = "";
            foreach (NetworkInterface item in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (item.NetworkInterfaceType == NetworkInterfaceType.Wireless80211 && item.OperationalStatus == OperationalStatus.Up)
                {
                    foreach (UnicastIPAddressInformation ip in item.GetIPProperties().UnicastAddresses)
                    {
                        if (ip.Address.AddressFamily == AddressFamily.InterNetwork)
                        {
                            output = ip.Address.ToString();
                        }
                    }
                }
            }
            Console.WriteLine("IP Address = " + output);

            Console.WriteLine("方法3");
        }
    }
}