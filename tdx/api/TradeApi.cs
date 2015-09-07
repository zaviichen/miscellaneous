using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Tdx
{
    class TradeApi
    {
        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void OpenTdx();

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void CloseTdx();

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern int Logon(string IP, short Port, string Version, string AccountNo, string JyPassword, string TxPassword, StringBuilder ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void Logoff(int ClientID);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void QueryData(int ClientID, int Category, StringBuilder Result, StringBuilder ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void SendOrder(int ClientID, int Category, int PriceType, string Gddm, string Zqdm, float Price, int Quantity, StringBuilder Result, StringBuilder ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void CancelOrder(int ClientID, string hth, StringBuilder Result, StringBuilder ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void GetQuote(int ClientID, string Zqdm, StringBuilder Result, StringBuilder ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void GetEdition(StringBuilder Result);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void QueryHistoryData(int ClientID, int Category, string StartDate, string EndDate, StringBuilder Result, StringBuilder ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void QueryDatas(int ClientID, int[] Category, int Count, IntPtr[] Result, IntPtr[] ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void SendOrders(int ClientID, int[] Category, int[] PriceType, string[] Gddm, string[] Zqdm, float[] Price, int[] Quantity, int Count, IntPtr[] Result, IntPtr[] ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void CancelOrders(int ClientID, string[] hth, int Count, IntPtr[] Result, IntPtr[] ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void GetQuotes(int ClientID, string[] Zqdm, int Count, IntPtr[] Result, IntPtr[] ErrInfo);

        [DllImport("trade.dll", CharSet = CharSet.Ansi)]
        public static extern void Repay(int ClientID, string Amount, StringBuilder Result, StringBuilder ErrInfo);

    }
}
