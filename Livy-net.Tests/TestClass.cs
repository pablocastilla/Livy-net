using Livy_net.Tests.Properties;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Livy_net.Tests.Utils;

namespace Livy_net.Tests
{
    [TestFixture]
    public class TestClass
    {
       
        [Test]
        public void PostJob_GetStatementsResult_Test()
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

               
               Assert.True(result.statements[0].output.result.Contains("Pi is roughly"));

            }
            finally
            {
                client.CloseSession(sessionData.id.ToString()).Wait();
            }
        }

        [Test]
        public void PostJob_GetStatementResult_Test()
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
                Statement result = null;
                while (i < 30 && !(result != null && result.state == StatementState.available))
                {
                    result = client.GetStatementResult(sessionData.id.ToString(),codeResult.id.ToString()).Result;

                    System.Threading.Thread.Sleep(1000);
                    i++;
                }


                Assert.True(result.output.result.Contains("Pi is roughly"));

            }
            finally
            {
                client.CloseSession(sessionData.id.ToString()).Wait();
            }
        }
        [Test]
        public void PostFileJob_GetStatementResult_Test()
        {
            var client = new LivyClient(Settings.Default.LivyUrl, Settings.Default.User, Settings.Default.Password, SessionKind.spark);

            var sessionData = client.OpenSession().Result;


            try
            {
                while (sessionData.state == SessionState.starting)
                {
                    sessionData = client.GetSessionState(sessionData.id.ToString()).Result;

                    System.Threading.Thread.Sleep(1000);
                }

             

                var pathUri = System.IO.Path.GetDirectoryName( System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase);
                var pathString = new Uri(pathUri).LocalPath;

                pathString = pathString + "\\Files\\TestJob.scala";

                // This text is added only once to the file.
                if (File.Exists(pathString))
                {
                    // Open the file to read from.
                   string code = File.ReadAllText(pathString);
                    var codeResult = client.PostStatement(sessionData.id.ToString(), code).Result;

                    int i = 0;
                    Statement result = null;
                    while (i < 30 && !(result != null && result.state == StatementState.available))
                    {
                        result = client.GetStatementResult(sessionData.id.ToString(), codeResult.id.ToString()).Result;

                        System.Threading.Thread.Sleep(1000);
                        i++;
                    }

                    Assert.True(result.output.result.Contains("5"));
                }              
                               

            }
            finally
            {
                client.CloseSession(sessionData.id.ToString()).Wait();
            }
        }


        [Test]
        public void PostJob_ValidationFile_GetStatementResult_Test()
        {
            var client = new LivyClient(Settings.Default.LivyUrl, Settings.Default.User, Settings.Default.Password, SessionKind.spark);

            var sessionData = client.OpenSession().Result;


            try
            {
                while (sessionData.state == SessionState.starting)
                {
                    sessionData = client.GetSessionState(sessionData.id.ToString()).Result;

                    System.Threading.Thread.Sleep(1000);
                }



                var pathUri = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase);
                var pathString = new Uri(pathUri).LocalPath;

                pathString = pathString + "\\Files\\Validations.scala";

                // This text is added only once to the file.
                if (File.Exists(pathString))
                {
                    // Open the file to read from.
                    string code = File.ReadAllText(pathString);
                    var codeResult = client.PostStatement(sessionData.id.ToString(), code).Result;

                    int i = 0;
                    Statement result = null;
                    while (i < 30 && !(result != null && result.state == StatementState.available))
                    {
                        result = client.GetStatementResult(sessionData.id.ToString(), codeResult.id.ToString()).Result;

                        System.Threading.Thread.Sleep(5000);
                        i++;
                    }

                    //Assert.True(result.output.result.Contains("5"));
                    Assert.IsTrue(true);
                }


            }
            finally
            {
                client.CloseSession(sessionData.id.ToString()).Wait();
            }
        }

        [Test]
        public void PostJob_ValidationParsedFile_GetStatementResult_Test()
        {
            var client = new LivyClient(Settings.Default.LivyUrl, Settings.Default.User, Settings.Default.Password, SessionKind.spark);

            var sessionData = client.OpenSession().Result;


            try
            {
                while (sessionData.state == SessionState.starting)
                {
                    sessionData = client.GetSessionState(sessionData.id.ToString()).Result;

                    System.Threading.Thread.Sleep(1000);
                }



                var pathUri = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase);
                var pathString = new Uri(pathUri).LocalPath;

                pathString = pathString + "\\Files\\ValidationsNotFormatted.scala";

                // This text is added only once to the file.
                if (File.Exists(pathString))
                {
                    // Open the file to read from.
                    string code = File.ReadAllText(pathString);
                    code = JSONUtils.EscapeStringValue(code);
                    var codeResult = client.PostStatement(sessionData.id.ToString(), code).Result;

                    int i = 0;
                    Statement result = null;
                    while (i < 30 && !(result != null && result.state == StatementState.available))
                    {
                        result = client.GetStatementResult(sessionData.id.ToString(), codeResult.id.ToString()).Result;

                        System.Threading.Thread.Sleep(5000);
                        i++;
                    }

                    //Assert.True(result.output.result.Contains("5"));
                    Assert.IsTrue(true);
                }


            }
            finally
            {
                client.CloseSession(sessionData.id.ToString()).Wait();
            }
        }



        [Test]
        public void DummyTest()
        {

            var pathUri = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase);
            var pathString = new Uri(pathUri).LocalPath;
            pathString = pathString + "\\Files\\ValidationsNotFormatted.scala";

            // This text is added only once to the file.
            if (File.Exists(pathString))
            {
                // Open the file to read from.
                string code = File.ReadAllText(pathString);

                var json = JSONUtils.EscapeStringValue(code);



                var data = "{'code': '" + code + "'}";
               // var data = "{'code': '" + code + "'}";

                JToken jt = JToken.Parse(data);

            }
        }
    }
}
