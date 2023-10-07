# ZmqBindlib
zmq常用封装

## 使用方法

基本使用
---------------------------
1.简单请求回复


`````
 ZmqRequest request = new ZmqRequest();
            request.RemoteAddress = localaddes;
            request.PubClient = "A";
            int num = 0;
            while (true)
            {
                //   Thread.Sleep(1000);
                //string msg = request.Request("hi");
                Person p=  request.Request<Person,Person>(new Person { Name = "jin", Description = "请求", Id = num++, Title = "rr" });
                Console.WriteLine(p.Description+p.Name);
            }
`````
`````
    ZmqResponse rep = new ZmqResponse();
            rep.LocalAddress = localaddes;
            rep.Start();
            int num = 0;
            //rep.ByteReceived += (sender, e) =>
            //{
            //    Console.WriteLine(System.Text.Encoding.Default.GetString(e));
            //    rep.Response("word"+num++);
            //};
            rep.StringReceived += (sender, e) =>
            {
                Console.WriteLine(e);
                if (e == "hi")
                {
                    Thread.Sleep(1000);
                }
                rep.Response("word" + num++);
            };
````````````  
			
2.异步下的请求回复，类似TCP，支持多请求

```````
  server =new EhoServer();
            server.RouterAddress = "tcp://127.0.0.1:66666";//服务地址，请求的远端地址
            //  server.ByteReceived += Server_ByteReceived;
          // server.StringReceived += Server_StringReceived1; 
            server.Start();
``````
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
		``````````````
		 private static  void recvice()
        {
            while (true)
            {
                var ss = server.GetMsg<Person>();
                ss.Message.Description = "回复"+ss.Message.Id;
                ss.Response(ss.Message);
            }
        }
		`````````````````
		
		-----------------------------
		订阅发布
		
		````
		  ZmqSubscriber sub = new ZmqSubscriber();
            sub.Address = new string[] { localaddes };
            sub.Subscribe("A");
           // sub.ByteReceived += Sub_ByteReceived;
            sub.StringReceived += Sub_StringReceived;
			```````````````
			```````````
			  ZmqPublisher pub = new ZmqPublisher();
            pub.LocalAddress =localaddes;
           // pub.IsProxy = true; 是否使用中间代理
            int num = 0;
            while (true)
            {
               // Thread.Sleep(1000);
                pub.Publish("A", "ssss"+num++);
            }
			````````````````
			
			```````````````
			static void Proxy()
        {
		//中间代理
            ZmqDDSProxy.PubAddress = "tcp://127.0.0.1:7771";//注意，客户端订阅此地址
            ZmqDDSProxy.SubAddress = "tcp://127.0.0.1:7772";//客户端发布此地址
            ZmqDDSProxy.Start();
        }
		````````````
		
		--------------------------
		说明
		1.接收数据一端，定义了2个事件一个方法，顺序是ByteReceived、StringReceived、GetMsg<T>()方法。一旦前一个实现，后面就无效

