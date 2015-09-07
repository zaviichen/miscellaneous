using log4net;
using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SQLite;
using Tdx.core;
using System.Globalization;
using Tdx.util;

namespace Tdx
{
    class MdRouter
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(MdRouter));

        public HashSet<string> Instruments = new HashSet<string>();
        private List<string[]> ChunkInstrumemts = null;
        private int chuckSize = 80;
        public int Interval = 2000;

        public StringBuilder Result = new StringBuilder(1024 * 1024);
        public StringBuilder ErrInfo = new StringBuilder(256);

        private NetMQContext context = NetMQContext.Create();
        private PublisherSocket publisher;
        private string pubTopic;

        private Trader trader;
        private TradeConfig config;
        private StreamWriter writer;
        private string ddb;
        private bool isInit = false;

        private Dictionary<string, Tick> prevTicks = new Dictionary<string, Tick>();

        public MdRouter(Trader trd)
        {
            trader = trd;
            config = trd.Config;
            ddb = config.Ddb;

            writer = new StreamWriter(DateTime.Now.ToString("yyyyMMddHH") + ".csv");

            publisher = context.CreatePublisherSocket();
            publisher.Bind(config.MarketPub.Address);
            pubTopic = config.MarketPub.GetTopic(0);
            logger.InfoFormat("bind publish address: {0}, topic: {1}", config.MarketPub.Address, pubTopic);

            try
            {
                string[] lines = File.ReadAllLines(config.UniverseFile);
                for (int i = 1; i < lines.Length; i++)
                {
                    string[] its = lines[i].Split(',');
                    string ric = its[0];
                    Instruments.Add(ric);
                }

                ChunkInstrumemts = Instruments
                    .Select((s, i) => new { Value = s, Index = i })
                    .GroupBy(x => x.Index / chuckSize)
                    .Select(grp => grp.Select(x => x.Value).ToArray())
                    .ToList();
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
            }

            isInit = Init();
        }

        public bool Init()
        {
            bool connectd = false;
            foreach (var server in config.MdServersList)
            {
                var its = server.Split(':');
                var address = its[0];
                var port = int.Parse(its[1]);
                connectd = MdApi.TdxL2Hq_Connect(address, port, Result, ErrInfo);
                if (connectd)
                {
                    logger.Info(string.Format("connected to md address: {0}:{1}", address, port));
                    break;
                }
            }

            if (connectd)
            {
                var ticks = BatchRequestTicks();
                if (ticks.Count > 0)
                {
                    ticks.ForEach(t => prevTicks[t.InstrumentId] = t);
                    logger.Info("init instruments=" + ticks.Count);
                }
                else
                {
                    logger.Error("no instruments received.");
                    return false;
                }
            }
            else
            {
                logger.Error("fail to connect the md servers.");
                return false;
            }
            return true;
        }

        public void Idle()
        {
            byte[] mkts = { 1 };
            string[] insts = { "000001" };
            short count = 1;
            MdApi.TdxL2Hq_GetSecurityQuotes(mkts, insts, ref count, Result, ErrInfo);
            logger.Debug("heartbeat: " + (ErrInfo.Length > 0 ? "error" : "live"));
        }

        public void Publish(Tick tick)
        {
            if (tick == null) return;
            try
            {
                prevTicks[tick.InstrumentId] = tick;
                string pb = Convert.ToBase64String(tick.ToByteArray());
                string msg = string.Format("{0}\t{1}", pubTopic, pb);
                publisher.Send(msg);
            }
            catch (Exception ex)
            {
                logger.Warn(string.Format("publish fail: {0}, tick={1}", ex.Message, tick.ToString()));
            }
        }

        private bool IsTickChange(Tick t)
        {
            if (!prevTicks.ContainsKey(t.InstrumentId))
            {
                return true;
            }
            var pt = prevTicks[t.InstrumentId];
            return (!Double.Equals(t.LastPrice, pt.LastPrice) ||
                t.Volume != pt.Volume ||
                t.Turnover > 0);
        }

        private Tick CreateTick(string content)
        {
            var e = content.Trim().Split('\t');
            try
            {
                var t = new Tick.Builder();
                t.InstrumentId = string.Format("{0}.{1}", e[1], (e[0].Equals("0") ? "SZ" : "SH"));

                if (t.InstrumentId.EndsWith("SZ") && trader.IsSZCloseAuction())
                {
                    // skip the sz close auction
                    return null;
                }

                t.Datetime = Utils.DatetimeToLong(trader.Now);
                t.Period = "t";
                bool first = !prevTicks.ContainsKey(t.InstrumentId);

                t.LastPrice = double.Parse(e[3]);
                //t.HistClose = double.Parse(e[4]);
                t.OpenPrice = double.Parse(e[5]);
                t.HighPrice = double.Parse(e[6]);
                t.LowPrice = double.Parse(e[7]);
                t.ClosePrice = t.LastPrice;

                var vol = long.Parse(e[10]) * 100;
                t.Volume = first ? vol : vol - prevTicks[t.InstrumentId].Volume;
                var tvr = double.Parse(e[12]);
                t.Turnover = first ? tvr : tvr - prevTicks[t.InstrumentId].Turnover;
                if (t.LastPrice <= 0 || t.Volume <= 0 || tvr <= 0 || t.Turnover < 0)
                {
                    return null;
                }

                var q = new Quote.Builder();
                int id = 17;
                for (int i = 0; i < 5; i++)
                {
                    q.AddBidPrice(double.Parse(e[id++]));
                    q.AddAskPrice(double.Parse(e[id++]));
                    q.AddBidVolume(long.Parse(e[id++]));
                    q.AddAskVolume(long.Parse(e[id++]));
                }
                t.Quote = q.Build();
                return t.Build();
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
                return null;
            }
        }

        private List<Tick> BatchRequestTicks()
        {
            List<Tick> ticks = new List<Tick>();
            foreach (var chuck in ChunkInstrumemts)
            {
                var ts = RequestTicks(chuck);
                ts.ForEach(t => ticks.Add(t));
            }
            return ticks;
        }

        private List<Tick> RequestTicks(string[] rics)
        {
            string[] insts = (from p in rics select p.Substring(0, p.Length - 3)).ToArray();
            byte[] mkts = (from p in rics select (p.EndsWith("SZ") ? (byte)0 : (byte)1)).ToArray();
            short count = (short)mkts.Length;
            List<Tick> ticks = new List<Tick>();
            try
            {
                bool status = MdApi.TdxL2Hq_GetSecurityQuotes(mkts, insts, ref count, Result, ErrInfo);
                if (status)
                {
                    var its = Result.ToString().Split('\n');
                    for (int i = 1; i < its.Length; i++)
                    {
                        var line = its[i];
                        var tick = CreateTick(line);
                        if (tick != null)
                        {
                            ticks.Add(tick);
                            writer.WriteLine(string.Format("{0}\t{1}", DateTime.Now.ToString("HH:mm:ss.fff"), line));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
            }
            return ticks;
        }

        public void Run()
        {
            if (!isInit)
            {
                logger.Error("init fails. exist.");
                return;
            }

            while (trader.TradeActive)
            {
                if (trader.IsMarketActive())
                {
                    Thread.Sleep(Interval);
                    foreach (var t in BatchRequestTicks())
                    {
                        Publish(t);
                    }
                }
                else
                {
                    Thread.Sleep(Interval * 2);
                    Idle();
                }
            }
        }

        public void DownloadDaliyQuote()
        {
            string insertT = "replace into quote (" +
                "date, symbol, open, high, low, close, volume, turnover, adjclose, gene) " +
                "values (@date, @symbol, @open, @high, @low, @close, @volume, @turnover, @adjclose, @gene)";

            using (SQLiteConnection conn = new SQLiteConnection(string.Format("Data Source={0}", ddb)))
            {
                conn.Open();
                using (var transaction = conn.BeginTransaction())
                using (var command = new SQLiteCommand(conn))
                {
                    command.CommandText = insertT;
                    foreach (var ric in Instruments)
                    {
                        Thread.Sleep(10);
                        logger.Info("updating bar: " + ric);
                        string inst = ric.Substring(0, ric.Length - 3);
                        byte mkt = (ric.EndsWith("SZ") ? (byte)0 : (byte)1);
                        short count = 3;

                        bool status = MdApi.TdxL2Hq_GetSecurityBars(4, mkt, inst, 0, ref count, Result, ErrInfo);
                        if (status)
                        {
                            var lines = Result.ToString().Split('\n');
                            for (int i = 1; i < lines.Length; i++)
                            {
                                var its = lines[i].Trim().Split('\t');
                                var _params = new SQLiteParameter[]
                                { 
                                    new SQLiteParameter("@date", its[0]), 
                                    new SQLiteParameter("@symbol", ric), 
                                    new SQLiteParameter("@open", its[1]), 
                                    new SQLiteParameter("@close", its[2]), 
                                    new SQLiteParameter("@high", its[3]), 
                                    new SQLiteParameter("@low", its[4]),
                                    new SQLiteParameter("@volume", its[5]),
                                    new SQLiteParameter("@turnover", its[6]),
                                    new SQLiteParameter("@adjclose", 0),
                                    new SQLiteParameter("@gene", 1)
                                };
                                command.Parameters.AddRange(_params);
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                    transaction.Commit();
                }
            }
            logger.Info("update daily bar finished.");
        }

        public void CreateDailyDB()
        {
            string createQuote = "create table if not exists quote (" +
			"date text, symbol text, " +
			"open real, high real, low real, close real, volume long, turnover long, adjclose real, gene real, " +
            "primary key(date, symbol))";

            string createDiv = "create table if not exists div (" +
			"date text, symbol text, " +
			"split real, right real, rightprc real, div real, " + 
            "primary key(date, symbol))";

            SQLiteConnection.CreateFile(ddb);
            using (SQLiteConnection conn = new SQLiteConnection(string.Format("Data Source={0}", ddb)))
            {
                conn.Open();
                using (var transaction = conn.BeginTransaction())
                using (var command = new SQLiteCommand(conn))
                {
                    command.CommandText = createQuote;
                    command.ExecuteNonQuery();
                    command.CommandText = createDiv;
                    command.ExecuteNonQuery();
                    transaction.Commit();
                }
            }
        }

        public void DownloadDiv()
        {
	        string insertT = "replace into div (" +
			"date, symbol, split, right, rightprc, div) " +
            "values (@date, @symbol, @split, @right, @rightprc, @div)";

            using (SQLiteConnection conn = new SQLiteConnection(string.Format("Data Source={0}", ddb)))
            {
                conn.Open();
                using (var transaction = conn.BeginTransaction())
                using (var command = new SQLiteCommand(conn))
                {
                    command.CommandText = insertT;
                    foreach (var ric in Instruments)
                    {
                        Thread.Sleep(10);
                        logger.Info("updating div: " + ric);
                        string inst = ric.Substring(0, ric.Length - 3);
                        byte mkt = (ric.EndsWith("SZ") ? (byte)0 : (byte)1);

                        bool status = MdApi.TdxL2Hq_GetXDXRInfo(mkt, inst, Result, ErrInfo);
                        if (status)
                        {
                            var lines = Result.ToString().Split('\n');
                            for (int i = 1; i < lines.Length; i++)
                            {
                                var its = lines[i].Trim().Split('\t');
                                var span = trader.Now - DateTime.ParseExact(its[2], "yyyyMMdd", CultureInfo.InvariantCulture);
                                if (span.TotalDays > 60)
                                {
                                    continue;
                                }

                                var _params = new SQLiteParameter[]
                                {
                                    // maybe wrong!
                                    new SQLiteParameter("@date", its[2]), 
                                    new SQLiteParameter("@symbol", ric),
                                    new SQLiteParameter("@split", its[4]), 
                                    new SQLiteParameter("@right", its[5]), 
                                    new SQLiteParameter("@rightprc", its[6]), 
                                    new SQLiteParameter("@div", its[7]) 
                                };
                                command.Parameters.AddRange(_params);
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                    transaction.Commit();
                }
                logger.Info("update div finished.");
            }
        }
    }
}
