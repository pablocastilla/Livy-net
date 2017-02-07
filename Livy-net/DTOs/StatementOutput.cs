using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net.DTOs
{
    public class StatementOutput
    {
        public string status { get; set; }

        public int execution_count { get; set; }

        public string data { get; set; }

        public string ename { get; set; }

        public string evalue { get; set; }
    }
}
