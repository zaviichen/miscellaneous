using Google.ProtocolBuffers;
using log4net;
using NetMQ;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Tdx.core;
using Tdx.util;

namespace Tdx
{
    class Program
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(Program));

        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                logger.Fatal("invalid args. exit.");
                return;
            }

            var trader = new Trader(args[0]);
            trader.Run();

            //TestTrade();
        }

        static void TestTrade()
        {
            //DLL是32位的,因此必须把C#工程生成的目标平台从Any CPU改为X86,才能调用DLL;
            //必须把Trade.dll等4个DLL复制到Debug或Release工程目录下;

            StringBuilder ErrInfo = new StringBuilder(256);
            StringBuilder Result = new StringBuilder(1024 * 1024);

            TradeApi.GetEdition(Result);//获取交易DLL版本
            Console.WriteLine(Result);

            TradeApi.OpenTdx();//打开通达信
            //int ClientID = TradeApi.Logon("222.73.56.70", 7708, "2.20", "39994126", "364866", string.Empty, ErrInfo);
            int ClientID = TradeApi.Logon("222.66.55.173", 7708, "2.20", "060000004868", "364866", string.Empty, ErrInfo);
            if (ClientID == -1)
            {
                Console.WriteLine(ErrInfo);
                return;
            }


            TradeApi.SendOrder(ClientID, 0, 0, "A000001", "601988", (float)2.5, 100, Result, ErrInfo);//下单
            Console.WriteLine("下单结果: {0}", Result);

            TradeApi.QueryData(ClientID, 0, Result, ErrInfo);
            Console.WriteLine("资金: {0}", Result);
            Console.WriteLine(ErrInfo);

            TradeApi.QueryData(ClientID, 2, Result, ErrInfo);
            Console.WriteLine("当日委托: {0}", Result);
            Console.WriteLine(ErrInfo);

            TradeApi.QueryData(ClientID, 3, Result, ErrInfo);
            Console.WriteLine("当日成交: {0}", Result);
            Console.WriteLine(ErrInfo);

            var its = Result.ToString().Split('\n');
            for (int i = 1; i < its.Length; i++)
            {
                var e = its[i].Split('\t');
                string id = e[10].Trim();
                var dtStr = string.Format("{0} {1}", DateTime.Now.ToString("yyyyMMdd"), e[0].Trim());
                Console.WriteLine(dtStr);
                var dt = DateTime.ParseExact(dtStr, "yyyyMMdd HH:mm:ss", CultureInfo.InvariantCulture);
                Console.WriteLine(Utils.DatetimeToLong(dt));
            }

            TradeApi.QueryHistoryData(ClientID,1,"20150310","20150310",Result,ErrInfo);
            Console.WriteLine(Result);
            Console.WriteLine(ErrInfo);


            //TradeApi.GetQuote(ClientID, "601988", Result, ErrInfo);//查询五档报价
            //if (ErrInfo.ToString() != string.Empty)
            //{
            //    Console.WriteLine(ErrInfo.ToString());
            //    return;
            //}
            //Console.WriteLine("行情结果: {0}", Result);

            //TradeApi.QueryData(ClientID, 0, Result, ErrInfo);//查询资金
            //if (ErrInfo.ToString() != string.Empty)
            //{
            //    Console.WriteLine(ErrInfo.ToString());
            //    return;
            //}
            //Console.WriteLine("查询结果: {0}", Result);

            ////批量查询多个证券的五档报价
            //string[] Zqdm = new string[] { "600030", "600031" };
            //string[] Results = new string[Zqdm.Length];
            //string[] ErrInfos = new string[Zqdm.Length];

            //IntPtr[] ResultPtr = new IntPtr[Zqdm.Length];
            //IntPtr[] ErrInfoPtr = new IntPtr[Zqdm.Length];

            //for (int i = 0; i < Zqdm.Length; i++)
            //{
            //    ResultPtr[i] = Marshal.AllocHGlobal(1024 * 1024);
            //    ErrInfoPtr[i] = Marshal.AllocHGlobal(256);
            //}


            //TradeApi.GetQuotes(ClientID, Zqdm, Zqdm.Length, ResultPtr, ErrInfoPtr);

            //for (int i = 0; i < Zqdm.Length; i++)
            //{
            //    Results[i] = Marshal.PtrToStringAnsi(ResultPtr[i]);
            //    ErrInfos[i] = Marshal.PtrToStringAnsi(ErrInfoPtr[i]);

            //    Marshal.FreeHGlobal(ResultPtr[i]);
            //    Marshal.FreeHGlobal(ErrInfoPtr[i]);
            //}


            TradeApi.Logoff(ClientID);//注销
            TradeApi.CloseTdx();//关闭通达信

            //Console.ReadLine();
        }
    }
}
