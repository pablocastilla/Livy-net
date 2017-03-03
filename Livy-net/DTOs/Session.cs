using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net
{ 

    /// <summary>
    /// A session represents an interactive shell
    /// </summary>
public class Session
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public SessionState state { get; set; }

        public int id { get; set; }

        /// <summary>
        /// spark	Interactive Scala Spark session
        /// pyspark Interactive Python 2 Spark session
        /// pyspark3 Interactive Python 3 Spark session
        /// sparkr Interactive R Spark session
        /// </summary>
        public string kind { get; set; }
                      
    }

    /// <summary>
    /// not_started Session has not been started
    /// starting Session is starting
    /// idle Session is waiting for input
    /// busy Session is executing a statement
    /// shutting_down Session is shutting down
    /// error Session errored out
    /// dead Session has exited
    /// success Session is successfully stopped
    /// </summary>
    public enum SessionState { not_started, starting, idle, busy, shutting_down, error, dead, success }
}
