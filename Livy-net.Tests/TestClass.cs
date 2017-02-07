using Livy_net.Tests.Properties;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net.Tests
{
    [TestFixture]
    public class TestClass
    {
       
        [Test]
        public void PostJobTest()
        {
            var client = new LivyClient(Settings.Default.LivyUrl, Settings.Default.User, Settings.Default.Password);

            var sessionData = client.OpenSession().Result;


            try
            {
                while (sessionData.state == SessionState.starting)
                {
                    sessionData = client.GetSessionState(sessionData.id.ToString()).Result;

                    System.Threading.Thread.Sleep(1000);
                }

                var code = "import random\n" +
                            "NUM_SAMPLES = 100000\n" +

                            "def sample(p):\n" +
                            "   x, y = random.random(), random.random()\n" +
                            "   return 1 if x * x + y * y < 1 else 0\n\n" +

                            "count = sc.parallelize(range(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)\n" +
                            "print (\"Pi is roughly % f\" % (4.0 * count / NUM_SAMPLES))";

                var codeResult = client.PostStatement(sessionData.id.ToString(), code).Result;

                int i = 0;
                Statements result=null;
                while ( i<30 && !(result!=null && result.statements[0].state==StatementState.available))
                {
                    result = client.GetStatementsResult(sessionData.id.ToString()).Result;

                    System.Threading.Thread.Sleep(1000);
                    i++;
                }

                var log = client.GetSessionLog(sessionData.id.ToString()).Result;

                Assert.True(result.statements[0].output.result.Contains("Pi is roughly"));

            }
            finally
            {
                client.CloseSession(sessionData.id.ToString()).Wait();
            }
        }
    }
}
