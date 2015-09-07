using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Tdx
{
    class MdApi
    {
        /// <summary>
        /// 获取逐笔成交数据
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Start">K线开始位置,最后一条K线位置是0, 前一条是1, 依此类推</param>
        /// <param name="Count">API执行前,表示用户要请求的K线数目, API执行后,保存了实际返回的K线数目</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetDetailTransactionData (byte Market, string Zqdm, short Start, ref short Count, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 批量获取多个证券的十档报价数据
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海, 第i个元素表示第i个证券的市场代码</param>
        /// <param name="Zqdm">证券代码, Count个证券代码组成的数组</param>
        /// <param name="Count">API执行前,表示用户要请求的证券数目,最大50, API执行后,保存了实际返回的数目</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetSecurityQuotes10 (byte[] Market, string[] Zqdm, ref short Count, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 获取买卖队列数据
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetBuySellQueue (byte Market, string Zqdm, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        ///  连接通达信行情服务器,服务器地址可在券商软件登录界面中的通讯设置中查得
        /// </summary>
        /// <param name="IP">服务器IP</param>
        /// <param name="Port">服务器端口</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_Connect(string IP, int Port, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 断开同服务器的连接
        /// </summary>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        public static extern void TdxL2Hq_Disconnect();

        /// <summary>
        /// 获取市场内所有证券的数量
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的证券数量</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool  TdxL2Hq_GetSecurityCount(byte Market, ref short Result, StringBuilder ErrInfo);

        /// <summary>
        /// 获取市场内从某个位置开始的1000支股票的股票代码
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Start">股票开始位置,第一个股票是0, 第二个是1, 依此类推,位置信息依据TdxL2Hq_GetSecurityCount返回的证券总数确定</param>
        /// <param name="Count">API执行后,保存了实际返回的股票数目,</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的证券代码信息,形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool  TdxL2Hq_GetSecurityList(byte Market, short Start, ref short Count, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 获取证券的K线数据
        /// </summary>
        /// <param name="Category">K线种类, 0->5分钟K线    1->15分钟K线    2->30分钟K线  3->1小时K线    4->日K线  5->周K线  6->月K线  7->1分钟    10->季K线  11->年K线< / param>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Start">K线开始位置,最后一条K线位置是0, 前一条是1, 依此类推</param>
        /// <param name="Count">API执行前,表示用户要请求的K线数目, API执行后,保存了实际返回的K线数目, 最大值800</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetSecurityBars(byte Category, byte Market, string Zqdm, short Start, ref short Count, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 获取指数的K线数据
        /// </summary>
        /// <param name="Category">K线种类, 0->5分钟K线    1->15分钟K线    2->30分钟K线  3->1小时K线    4->日K线  5->周K线  6->月K线  7->1分钟  8->1分钟K线  9->日K线  10->季K线  11->年K线< / param>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Start">K线开始位置,最后一条K线位置是0, 前一条是1, 依此类推</param>
        /// <param name="Count">API执行前,表示用户要请求的K线数目, API执行后,保存了实际返回的K线数目, 最大值800</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetIndexBars(byte Category, byte Market, string Zqdm, short Start, ref short Count, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 获取分时数据
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetMinuteTimeData(byte Market, string Zqdm, StringBuilder Result, StringBuilder ErrInfo);


        /// <summary>
        /// 获取历史分时数据
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Date">日期, 比如2014年1月1日为整数20140101</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetHistoryMinuteTimeData(byte Market, string Zqdm, int Date, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 获取F10资料的分类
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        /// 
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetCompanyInfoCategory(byte Market, string Zqdm, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 获取F10资料的某一分类的内容
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="FileName">类目的文件名, 由TdxL2Hq_GetCompanyInfoCategory返回信息中获取</param>
        /// <param name="Start">类目的开始位置, 由TdxL2Hq_GetCompanyInfoCategory返回信息中获取</param>
        /// <param name="Length">类目的长度, 由TdxL2Hq_GetCompanyInfoCategory返回信息中获取</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据,出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetCompanyInfoContent(byte Market, string Zqdm, string FileName, int Start, int Length, StringBuilder Result, StringBuilder ErrInfo);

        /// <summary>
        /// 获取除权除息信息
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据,出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetXDXRInfo(byte Market, string Zqdm, StringBuilder Result, StringBuilder ErrInfo);



        /// <summary>
        /// 获取财务信息
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据,出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetFinanceInfo(byte Market, string Zqdm, StringBuilder Result, StringBuilder ErrInfo);



        /// <summary>
        /// 获取分时成交数据
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Start">K线开始位置,最后一条K线位置是0, 前一条是1, 依此类推</param>
        /// <param name="Count">API执行前,表示用户要请求的记录数目, API执行后,保存了实际返回的记录数目</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetTransactionData(byte Market, string Zqdm, short Start, ref short Count, StringBuilder Result, StringBuilder ErrInfo);



        /// <summary>
        /// 获取历史分时成交数据
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Start">K线开始位置,最后一条K线位置是0, 前一条是1, 依此类推</param>
        /// <param name="Count">API执行前,表示用户要请求的记录数目, API执行后,保存了实际返回的记录数目</param>
        /// <param name="Date">日期, 比如2014年1月1日为整数20140101</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetHistoryTransactionData(byte Market, string Zqdm, short Start, ref short Count, int Date, StringBuilder Result, StringBuilder ErrInfo);



        /// <summary>
        /// 获取五档报价
        /// </summary>
        /// <param name="Market">市场代码,   0->深圳     1->上海</param>
        /// <param name="Zqdm">证券代码</param>
        /// <param name="Count">API执行前,表示证券代码的记录数目, API执行后,保存了实际返回的记录数目</param>
        /// <param name="Result">此API执行返回后，Result内保存了返回的查询数据, 形式为表格数据，行数据之间通过\n字符分割，列数据之间通过\t分隔。一般要分配1024*1024字节的空间。出错时为空字符串。</param>
        /// <param name="ErrInfo">此API执行返回后，如果出错，保存了错误信息说明。一般要分配256字节的空间。没出错时为空字符串。</param>
        /// <returns>成功返货true, 失败返回false</returns>
        [DllImport("TdxHqApi.dll", CharSet = CharSet.Ansi)]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool TdxL2Hq_GetSecurityQuotes(byte[] Market, string[] Zqdm, ref short Count, StringBuilder Result, StringBuilder ErrInfo);
    }
}
