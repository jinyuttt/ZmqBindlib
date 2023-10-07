using ZmqBindlib;

namespace ConsoleApp1
{
    internal class Program
    {
        static string localaddes = "tcp://127.0.0.1:5560";
       static EhoServer server= new EhoServer();
        static void Main(string[] args)
        {
              Rep();
              Req();
           // Sub();
          //  pub();
            Console.WriteLine("Hello, World!");
            recvice();
            Console.ReadLine();
        }

        static void Req()
        {
            Thread thread = new Thread(Req1);

            thread.Start();

            Thread thread1 = new Thread(Req2);

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
                //   Thread.Sleep(1000);
                //string msg = request.Request("hi");
               request.Request<Person,string>(new Person { Name = "jin", Description = "请求", Id = num++, Title = "rr" });
               // Console.WriteLine(p.Description+p.Name);
            }

        }
        static void Req2()
        {
         

         
            //ZmqRequest request = new ZmqRequest();
            //request.RemoteAddress = localaddes;
            //request.PubClient = "B";
            //int num = 0;
            //while (true)
            //{
            //   //  Thread.Sleep(1000);
            //    Person p = request.Request<Person, Person>(new Person { Name = "yu", Description = "请求", Id = num++, Title = "ss" });
            //    // string msg = request.Request("hello");
            //    Console.WriteLine(p.Description+p.Name);
            //}

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
            server.IsEmptyReturn = true;
           // server.RouterAddress = "tcp://127.0.0.1:66666";//服务地址，请求的远端地址
            //  server.ByteReceived += Server_ByteReceived;
          // server.StringReceived += Server_StringReceived1; 
            server.Start();
           
           
        }

        private static  void recvice()
        {
            while (true)
            {
                var ss = server.GetMsg<Person>();
                ss.Message.Description = "回复"+ss.Message.Id;
                //ss.Response(ss.Message);
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
            sub.Address = new string[] { localaddes };
            sub.Subscribe("A");
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
            pub.LocalAddress =localaddes;
           // pub.IsProxy = true; 是否使用中间代理
            int num = 0;
            while (true)
            {
               // Thread.Sleep(1000);
                pub.Publish("A", "ssss"+num++);
            }

        

        }
        static void Proxy()
        {
            ZmqDDSProxy.PubAddress = "tcp://127.0.0.1:7771";//注意，客户端订阅此地址
            ZmqDDSProxy.SubAddress = "tcp://127.0.0.1:7772";//客户端发布此地址
            ZmqDDSProxy.Start();
        }
    }
}