using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net
{
    public class Session
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public SessionState state { get; set; }

        public int id { get; set; }

        public string kind { get; set; }
                      
    }

    public enum SessionState { not_started, starting, idle, busy, shutting_down, error, dead, success }
}
