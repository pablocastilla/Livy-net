using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net
{
    public class BatchesResponse
    {
        public string from { get; set; }

        public string total { get; set; }

        public List<Batch> sessions { get; set; }
    }
}
