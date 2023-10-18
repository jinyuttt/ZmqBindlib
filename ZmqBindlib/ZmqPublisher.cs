﻿using NetMQ;
using NetMQ.Sockets;

namespace MQBindlib
{
    /// <summary>
    /// 发布
    /// </summary>
    public class ZmqPublisher
    {
        PublisherSocket publisherSocket = null;
        ResponseSocket rsp = null;
        /// <summary>
        /// 本地地址
        /// </summary>
        public string? Address { get; set; }

        /// <summary>
        /// 标识
        /// </summary>
        public string? PubClient { get; set; } = string.Empty;

        /// <summary>
        /// 是否使用代理，使用则LocalAddress是代理地址
        /// </summary>
        public bool IsProxy { get; set; } = false; 

        //有主从
        public bool IsDDS { get; set; }=false;


        

        List<ClusterNode> lstNode = new List<ClusterNode>();

        DateTime fulshTime = DateTime.Now;

        private readonly TimeSpan m_deadNodeTimeout = TimeSpan.FromSeconds(10);

        public string LocalIP { get; set; }="*";

        public int LocalPort { get; set; } = 0;

        private List<string> lstLocalIP=new List<string>();

        private string m_localIP;
       
        private void Update()
        {
            rsp= new ResponseSocket();
            m_localIP = LocalIP;
            if (LocalPort < 1)
            {
                int port = rsp.BindRandomPort("tcp://"+LocalIP);
                LocalPort = port;
            }
            else
            {
                rsp.Bind(string.Format("tcp://{0}:{1}", LocalIP, LocalPort));
            }
            if(LocalIP=="*")
            {
                m_localIP=Util.GetLocalIP();
            }
            
         
            Thread rc = new Thread(p =>
            {
                while(true)
                {
                   var msg =  rsp.ReceiveFrameString();
                 
                    List<ClusterNode> lst = Util.JSONDeserializeObject<List<ClusterNode>>(msg);
                    lstNode = lst;
                    fulshTime = DateTime.Now;
                    rsp.SendFrameEmpty();
                 
                }
            });
            rc.Start();
            Thread up = new Thread(p =>
            {
               
                while (true)
                {
                    Thread.Sleep(1000);
                    string addr = string.Format("tcp://{0}:{1}",m_localIP,LocalPort);
                    try
                    {
                        publisherSocket.SendMoreFrame(ConstString.PubPublisher).SendFrame(addr);
                    }
                    catch { };
                    if (lstNode != null)
                    {
                        var master = lstNode.Find(p => p.IsMaster);
                        if (master != null)
                        {
                            if (master.SubAddess != Address || DateTime.Now > fulshTime + m_deadNodeTimeout)
                            {

                                try
                                {
                                    publisherSocket.Disconnect(Address);
                                }
                                catch { }
                                if (Address == master.SubAddess)
                                {
                                    try
                                    {
                                        //超时，切换地址
                                        var tmp = lstNode.Where(p => p.Id != master.Id).OrderByDescending(p => p.Id).First();
                                        //没有新节点理论上一致，先切换
                                        Address = tmp.SubAddess;
                                        publisherSocket.Connect(Address);
                                        Console.WriteLine("pub切换:" + Address);
                                    }
                                    catch(Exception ex)
                                    {
                                        Console.WriteLine(ex);
                                    }

                                }
                                else
                                {
                                    //master切换
                                  
                                    Address = master.SubAddess;
                                    publisherSocket.Connect(Address);
                                    Console.WriteLine("pub切换:" + Address);
                                }


                            }
                        }
                    }
                }
            });
            up.Start();
        }

        /// <summary>
        /// 发布数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        public void Publish<T>(string topic, T message)
        {
            if (publisherSocket == null)
            {
                publisherSocket = new PublisherSocket();
                publisherSocket.Options.SendHighWatermark = 0;
                publisherSocket.Options.TcpKeepalive = true;
                publisherSocket.Options.HeartbeatInterval = new TimeSpan(10000);
                publisherSocket.Options.HeartbeatTimeout = new TimeSpan(0, 0, 10);
                publisherSocket.Options.HeartbeatTtl = new TimeSpan(0, 0, 10);

                if (IsProxy)
                {
                    publisherSocket.Connect(Address);
                    if (IsDDS)
                    {

                        Update();
                    }
                }
                else
                {
                    publisherSocket.Bind(Address);
                }
            }
            var msg = Util.JSONSerializeObject(message);

            publisherSocket.SendMoreFrame(topic).SendFrame(msg);



        }

    }
}
