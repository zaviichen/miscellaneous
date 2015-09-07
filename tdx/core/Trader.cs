using Google.ProtocolBuffers;
using log4net;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Tdx.core
{
    class Trader
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(Trader));

        public TradeConfig Config { get; set; }
        private MdRouter md;
        private TradeRouter trade;

        private DateTime sod = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day);
        private DateTime AmOpen;
        private DateTime AmClose;
        private DateTime PmOpen;
        private DateTime PmClose;
        private DateTime TradeStart;
        private DateTime TradeStop;
        private DateTime PostTrade;
        private DateTime ExitTime;
        private DateTime SZCloseAuction;

        public string TdayStr
        {
            get { return DateTime.Now.ToString("yyyyMMdd"); }
        }

        public Trader(string conf)
        {
            TradeConfig.Builder builder = TradeConfig.CreateBuilder();
            TextFormat.Merge(File.OpenText(conf).ReadToEnd(), builder);
            Config = builder.Build();
            logger.Info("load Config success: " + conf);

            AmOpen = sod + TimeSpan.Parse(Config.AmOpen);
            AmClose = sod + TimeSpan.Parse(Config.AmClose);
            PmOpen = sod + TimeSpan.Parse(Config.PmOpen);
            PmClose = sod + TimeSpan.Parse(Config.PmClose);
            SZCloseAuction = sod + TimeSpan.Parse("14:56:55");

            TradeStart = sod + TimeSpan.Parse(Config.TradeStart);
            TradeStop = sod + TimeSpan.Parse(Config.TradeStop);
            PostTrade = sod + TimeSpan.Parse(Config.PostTrade);
            ExitTime = sod + TimeSpan.Parse(Config.ExitTime);

            md = new MdRouter(this);
            trade = new TradeRouter(this);

            TradeActive = false;
            PostTradeDone = false;
        }

        public DateTime Now
        {
            get { return DateTime.Now; }
        }

        public bool TradeActive { get; set; }
        public bool PostTradeDone { get; set; }

        public bool IsMarketActive()
        {
            return ((Now >= AmOpen) && (Now <= AmClose)) || ((Now >= PmOpen) && (Now <= PmClose));
        }

        public bool IsSZCloseAuction()
        {
            return (Now >= SZCloseAuction) && (Now <= PmClose);
        }

        public void Run()
        {
            while(true)
            {
                Thread.Sleep(100);
                if (Now >= TradeStart && Now < TradeStop && !TradeActive)
                {
                    TradeActive = true;
                    logger.Info("start market data thread...");
                    (new Thread(md.Run)).Start();

                    logger.Info("start trade thread...");
                    (new Thread(trade.Run)).Start();
                }

                if (Now >= TradeStop && TradeActive)
                {
                    logger.Info("cancel all live orders.");
                    trade.CancelOrders();

                    TradeActive = false;
                    logger.Info("stop market data thread...");
                    logger.Info("stop trade thread...");
                }

                if (Now >= PostTrade && !PostTradeDone)
                {
                    logger.Info("query today's trades.");
                    trade.QueryDailyTrades();

                    if (!File.Exists(Config.Ddb))
                    {
                        logger.Info("ddb not exists, create a new one.");
                        md.CreateDailyDB();
                    }

                    logger.Info("download daily bar quote.");
                    md.DownloadDaliyQuote();

                    logger.Info("download div.");
                    md.DownloadDiv();
                    PostTradeDone = true;
                }

                if (Now >= ExitTime)
                {
                    logger.Info("exit the program.");
                    break;
                }
            }
        }
    }
}
