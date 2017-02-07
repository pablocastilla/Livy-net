using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net
{
    public class Batch
    {
        public int id { get; set; }

        public string state { get; set; }

        public string[] log { get; set; }
    }
}
