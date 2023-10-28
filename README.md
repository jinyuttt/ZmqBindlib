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
```````
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
``````
```````
		 private static  void recvice()
        {
            while (true)
            {
                var ss = server.GetMsg<Person>();
                ss.Message.Description = "回复"+ss.Message.Id;
                ss.Response(ss.Message);
            }
        }

```````
3.订阅发布
		
		````
		  ZmqSubscriber sub = new ZmqSubscriber();
            sub.Address = new string[] { localaddes };
            sub.Subscribe("A");
           // sub.ByteReceived += Sub_ByteReceived;
            sub.StringReceived += Sub_StringReceived;
````
````
			  ZmqPublisher pub = new ZmqPublisher();
            pub.LocalAddress =localaddes;
           // pub.IsProxy = true; 是否使用中间代理
            int num = 0;
            while (true)
            {
               // Thread.Sleep(1000);
                pub.Publish("A", "ssss"+num++);
            }
`````
`````
			static void Proxy()
        {
		//中间代理
            ZmqDDSProxy.PubAddress = "tcp://127.0.0.1:7771";//注意，客户端订阅此地址
            ZmqDDSProxy.SubAddress = "tcp://127.0.0.1:7772";//客户端发布此地址
            ZmqDDSProxy.Start();
        }
        ``````````

## 中心高可用部署
1.推荐方式
  使用IP漂移：
  1. windows  
     使用DNS+VLS；Panguha软件
  2.Linux
     使用keppalive

2.使用封装
  该功能前提是可以使用广播，可以允许少量数据丢失；
  （1）请求返回模式
      中心节点：

       ```````````

          EhoServer eho = new EhoServer();
            eho.IsCluster = true;
            eho.DealerAddress = "inproc://server";
            eho.RouterAddress = "tcp://127.0.0.1:5550";

            eho.StringReceived += EhoServer_StringReceived;
            eho.Start();

      `````````````

 客户端：与单个一致

（2）订阅发布

 中心：

`````````````
      ZmqDDSProxy.PubAddress = "tcp://127.0.0.1:2222";
            ZmqDDSProxy.SubAddress = "tcp://127.0.0.1:4444";
            ZmqDDSProxy.IsCluster=true;
         ZmqDDSProxy.Start();

`````````````

 发布端：


`````````````
            ZmqPublisher pub = new ZmqPublisher();
            pub.Address = "tcp://127.0.0.1:5678";
            pub.IsProxy = true; //是否使用中间代理
            pub.IsDDS = true;//高可用启动
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



`````````````

  订阅端：


`````````````
              ZmqSubscriber sub = new ZmqSubscriber();
            sub.Address = new string[] { "tcp://127.0.0.1:1234" };
            sub.IsDDS = true;//高可用启动
            sub.Subscribe("");
           // sub.ByteReceived += Sub_ByteReceived;
            sub.StringReceived += Sub_StringReceived;

`````````````
  对于发布订阅，中心何发布订阅端都需要启动高可用，会刷新地址

 （3）负载均衡式订阅发布
    该模式是仿照kafka功能的；
     中心：
```
               ZmqPullProxy.PubAddress = "tcp://127.0.0.1:2222";
            ZmqPullProxy.SubAddress = "tcp://127.0.0.1:4444";
            ZmqPullProxy.IsCluster = true;//高可用
            ZmqPullProxy.Start(); //注意方法，启动和另外发布订阅方法不同

```
 发布端：和前面一样
 订阅端：
```
           ZmqSubscriberGroup zmqSubscriber=new ZmqSubscriberGroup();
            zmqSubscriber.Address = "tcp://127.0.0.1:1234";
            zmqSubscriber.IsDDS= true;//高可用
           // zmqSubscriber.Indenty = "test";//订阅在不同分组
            zmqSubscriber.Subscribe("A");
            zmqSubscriber.StringReceived += ZmqSubscriber_StringReceived;
```
(4)kafka封装
```
             KafkaPublisher kafkaPublisher = new KafkaPublisher();
                int num = 0;
                while (true)
                {
                    Thread.Sleep(1000);
                    kafkaPublisher.Push("A", "SSSSS"+num++);
                }
```
```
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
```
说明
		1.接收数据一端，定义了2个事件一个方法，顺序是ByteReceived、StringReceived、GetMsg<T>()方法。一旦前一个实现，后面就无效
        2.pull模式订阅增加了数据存储

