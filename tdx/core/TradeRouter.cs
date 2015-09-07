using log4net;
using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tdx.core;
using Tdx.util;

namespace Tdx
{
    class TradeRouter
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(TradeRouter));

        private StringBuilder Result = new StringBuilder(1024 * 1024);
        private StringBuilder ErrInfo = new StringBuilder(256);

        private NetMQContext context = NetMQContext.Create();
        private PublisherSocket pub;
        private string pubTopic;
        private SubscriberSocket sub;

        private Trader trader;
        private TradeConfig config;
        private int client;
        private bool isInit = false;
        private Dictionary<string, Order> sendOrders = new Dictionary<string, Order>();

        public TradeRouter(Trader trd)
        {
            trader = trd;
            config = trader.Config;

            try
            {
                pub = context.CreatePublisherSocket();
                pub.Bind(config.OrderPub.Address);
                pubTopic = config.OrderPub.TopicList[0];
                logger.InfoFormat("bind pub address: {0}, topic: {1}", config.OrderPub.Address, pubTopic);

                sub = context.CreateSubscriberSocket();
                sub.Connect(config.OrderSub.Address);
                logger.Info("bind sub address: " + config.OrderSub.Address);
                foreach (var topic in config.OrderSub.TopicList)
                {
                    sub.Subscribe(topic);
                    logger.Info("subscribe topic: " + topic);
                }
            }
            catch(Exception ex)
            {
                logger.Error(ex.Message);
            }

            isInit = Init();
        }

        // Not use
        //public ~TradeRouter()
        //{
        //    TradeApi.Logoff(client);
        //    TradeApi.CloseTdx();
        //}

        public bool Init()
        {
            TradeApi.OpenTdx();

            string[] its = config.TradeServer.Split(':');
            string address = its[0];
            short port = short.Parse(its[1]);

            client = TradeApi.Logon(address, port, "2.20", config.FundingAccount, config.TradePasswd, config.CommunicatePasswd, ErrInfo);
            logger.Info(string.Format(
                "trade login: {0}:{1},fund_acct={2},sh_acct={3},sz_acct={4},trade_passwd={5},com_passwd={6}",
                address, port, config.FundingAccount, config.ShAccount, config.SzAccount, config.TradePasswd, config.CommunicatePasswd));
            
            if (client == -1)
            {
                logger.Error("login fails:" + ErrInfo);
                return false;
            }
            else
            {
                logger.Info("login success: client=" + client.ToString());
                return true;
            }
        }

        public void SendOrder(string msg)
        {
            try
            {
                Order o = Order.ParseFrom(Convert.FromBase64String(msg));
                int dir = (o.Direction == Order.Types.Direction.BUY) ? 0 : 1;
                string id = SendOrder(o.Instrument, dir, o.LimitPrice, o.Volume);
                if (id != null)
                {
                    sendOrders.Add(id, o);
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
                return;
            }
        }

        private string SendOrder(string ric, int dir, double price, long size)
        {
            logger.Info(string.Format(
                "send order: ric={0},dir={1},price={2},size={3}", ric, (dir == 0) ? "B" : "S", price, size));

            string[] its = ric.Split('.');
            string inst = its[0];
            bool isSH = its[1].ToUpper().Equals("SH");

            string account = isSH ? config.ShAccount : config.SzAccount;
            TradeApi.SendOrder(client, dir, 0, account, inst, (float)price, (int)size, Result, ErrInfo);

            if (ErrInfo.Length > 0)
            {
                logger.Error("order fails: " + ErrInfo);
                return null;
            }
            else
            {
                logger.Info("order status: " + Result);
                var res = Result.ToString().Split('\n');
                var orderId = res[1].Split('\t')[0].Trim();
                return orderId;
            }
        }

        private void PublishOrderRsp(bool status, Order o)
        {
            if (o == null) return;
            try
            {
                var rsp = new OrderStatus.Builder();
                if (status)
                {
                    rsp.SetRspStatus(OrderStatus.Types.RspStatus.SUCCESS);
                    rsp.SetStatus(OrderStatus.Types.Status.Filled);
                    rsp.SetId(o.Id);
                    rsp.SetUpdateTime(trader.Now.Ticks);
                    rsp.SetFilled(o.Volume);
                    rsp.SetRemaining(0);
                    rsp.SetAvgFillPrice(o.LimitPrice);
                    rsp.SetLastFillPrice(o.LimitPrice);
                }
                else
                {
                    rsp.SetRspStatus(OrderStatus.Types.RspStatus.ERROR);
                    rsp.SetStatus(OrderStatus.Types.Status.Inactive);
                    rsp.SetId(o.Id);
                    rsp.SetUpdateTime(trader.Now.Ticks);
                    rsp.SetFilled(0);
                    rsp.SetRemaining(o.Volume);
                    rsp.SetAvgFillPrice(0);
                    rsp.SetLastFillPrice(0);
                }
                string pb = Convert.ToBase64String(rsp.Build().ToByteArray());
                string msg = string.Format("Tdx:All:OrderRsp\t{0}", pb);
                pub.Send(msg);
                logger.Info("Send order response: " + o.Instrument);
            }
            catch (Exception ex)
            {
                logger.Error(string.Format("publish fail: {0}", ex.Message));
            }
        }

        public void Run()
        {
            if (!isInit)
            {
                logger.Fatal("init fails. exist.");
                return;
            }

            while (trader.TradeActive)
            {
                string msg = sub.ReceiveString(Encoding.UTF8);
                string[] its = msg.Split('\t');
                //string topic = its[0].ToLower();
                string topic = its[0];
                string pb = its[1];

                if (topic.Equals("Tdx:Req:Order"))
                {
                    SendOrder(pb);
                }
                else
                {
                    logger.Error("unknown topic:" + topic);
                }
            }
        }

        public void CancelOrders()
        {
            var orderIds = new HashSet<string>();
            try
            {
                TradeApi.QueryData(client, 4, Result, ErrInfo);
                var its = Result.ToString().Split('\n');
                for (int i = 1; i < its.Length; i++)
                {
                    var line = its[i];
                    var e = line.Split('\t');
                    orderIds.Add(e[8]);
                }
                foreach (var id in orderIds)
                {
                    TradeApi.CancelOrder(client, id, Result, ErrInfo);
                    if (ErrInfo.Length > 0)
                    {
                        logger.Error("cancel order fails: " + ErrInfo);
                    }
                }
                logger.InfoFormat("cancel {0} orders.", orderIds.Count);
            }
            catch(Exception ex)
            {
                logger.Error("cancel order exception: " + ex.Message);
            }
        }

        public void QueryDailyTrades()
        {
            // 国金:
            //成交时间	证券代码	证券名称	买卖标志	买卖标志	状态说明	成交价格	成交数量	成交金额	成交编号	委托编号	股东代码	撤单标志	保留信息
            //14:47:15	000917	电广传媒	0	买入	成交	20.070	100	2007.00	410968	406104	0136062971	0

            // 华宝:
            //成交时间	股东代码	证券代码	证券名称	买卖标志	买卖标志	成交价格	成交数量	发生金额	成交编号	撤单标志	委托编号	保留信息
            TradeApi.QueryData(client, 3, Result, ErrInfo);

            var its = Result.ToString().Split('\n');
            for (int i = 1; i < its.Length; i++)
            {
                try
                {
                    logger.Info("get filled trade: " + its[i]);
                    var e = its[i].Split('\t');
                    string delId = e[11].Trim();
                    if (!sendOrders.ContainsKey(delId))
                    {
                        logger.Error("can not find the order delegation id = " + delId);
                        continue;
                    }

                    var o = sendOrders[delId];
                    var rsp = new OrderStatus.Builder();
                    rsp.SetRspStatus(OrderStatus.Types.RspStatus.SUCCESS);
                    rsp.SetStatus(OrderStatus.Types.Status.Filled);
                    rsp.SetId(o.Id);

                    var dtStr = string.Format("{0} {1}", trader.TdayStr, e[0].Trim());
                    var dt = DateTime.ParseExact(dtStr, "yyyyMMdd HH:mm:ss", CultureInfo.InvariantCulture);
                    rsp.SetUpdateTime(Utils.DatetimeToLong(dt));

                    long qdone = long.Parse(e[7]);
                    rsp.SetFilled(qdone);
                    rsp.SetRemaining(o.Volume - qdone);

                    double price = double.Parse(e[6]);
                    rsp.SetAvgFillPrice(price);
                    rsp.SetLastFillPrice(price);

                    string pb = Convert.ToBase64String(rsp.Build().ToByteArray());
                    string msg = string.Format("{0}\t{1}", pubTopic, pb);
                    pub.Send(msg);
                    logger.Info("Send order response: " + o.Instrument);
                }
                catch (Exception ex)
                {
                    logger.Error(ex.Message);
                }
            }
        }
    }
}
