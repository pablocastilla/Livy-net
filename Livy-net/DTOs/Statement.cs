using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net
{
    public class Statement
    {
        public int id { get; set; }
        
        /// <summary>
        ///  waiting	Statement is enqueued but execution hasn't started
        ///running Statement is currently running
        ///available Statement has a response ready
        ///error Statement failed
        ///cancelling Statement is being cancelling
        ///cancelled Statement is cancelled
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]        
        public StatementState state { get; set; }

        public Output output { get; set; }
                         
    }

    public enum StatementState { waiting , running , available , error , cancelling , cancelled }
}
