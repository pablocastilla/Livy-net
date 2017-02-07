using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net
{
    public class Output
    {
        public string status { get; set; }

        public int execution_count { get; set; }

        public dynamic data { get; set; }

        public string result
        {
            get
            {
                if (data != null)
                    return data["text/plain"].Value;
                else
                    return null;
            }
        }

        public string ename { get; set; }

        public string evalue { get; set; }

    }

}
