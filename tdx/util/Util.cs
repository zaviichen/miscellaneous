using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tdx.util
{
    public static class Extension
    {
        public static string EncodingConvert(string fromString, Encoding fromEncoding, Encoding toEncoding)
        {
            byte[] fromBytes = fromEncoding.GetBytes(fromString);
            byte[] toBytes = Encoding.Convert(fromEncoding, toEncoding, fromBytes);

            string toString = toEncoding.GetString(toBytes);
            return toString;
        }

        public static string ToGB2312(this string utf8String)
        {
            Encoding fromEncoding = Encoding.UTF8;
            Encoding toEncoding = Encoding.GetEncoding("gb2312");
            return EncodingConvert(utf8String, fromEncoding, toEncoding);
        }
    }

    public class Utils
    {
        public static long DatetimeToLong(DateTime dt)
        {
            DateTime offset = new DateTime(1970, 1, 1);
            TimeSpan utc = dt - offset;
            return (long)utc.TotalMilliseconds;
        }
    }
}
