using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net.Tests.Utils
{
    public class JSONUtils
    {
        public static string EscapeStringValue(string value)
        {
            const char BACK_SLASH = '\\';
            const char SIMPLE_SLASH = '\'';
            

            var output = new StringBuilder(value.Length);
            foreach (char c in value)
            {
                switch (c)
                {
                    case SIMPLE_SLASH:
                        output.AppendFormat("{0}{1}", BACK_SLASH, SIMPLE_SLASH);
                        break;

                    case BACK_SLASH:
                        output.AppendFormat("{0}{0}", BACK_SLASH);
                        break;

                    default:
                        output.Append(c);
                        break;
                }
            }

            return output.ToString();
        }
    
    }
}
