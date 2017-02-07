using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Livy_net
{
    public enum SessionKind {spark,pyspark,pyspark3,sparkr};

    public class LivyClient
    {
        private string livyUrl;
        private string user;
        private string password;
        private SessionKind kind;

        public LivyClient(string livyUrl, string user, string password, SessionKind kind = SessionKind.pyspark3)
        {
            this.livyUrl = livyUrl;
            this.user = user;
            this.password = password;
            this.kind = kind;
        }

        
        public async Task<Session> OpenSession()
        {

            var data = "{'kind': '"+kind.ToString()+"'}";


            Session response = null;
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(livyUrl);

                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var byteArray = Encoding.ASCII.GetBytes(user + ":" + password);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));


                JToken jt = JToken.Parse(data);
                string formattedJson = jt.ToString();


                HttpResponseMessage result = await client.PostAsync("/livy/sessions", new StringContent(formattedJson, Encoding.UTF8, "application/json"));

                if (result.IsSuccessStatusCode)
                {
                    response = await result.Content.ReadAsAsync<Session>().ConfigureAwait(false);

                }

            }

            return response;
        }

        public async Task<Session> GetSessionState(string sessionId)
        {

            Session response = null;

            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(livyUrl);

                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var byteArray = Encoding.ASCII.GetBytes(user + ":" + password);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
                                              
                HttpResponseMessage result = await client.GetAsync("/livy/sessions/"+sessionId);

                if (result.IsSuccessStatusCode)
                {
                    response = await result.Content.ReadAsAsync<Session>().ConfigureAwait(false);

                }

            }

            return response;
        }

        public async Task CloseSession(string sessionId)
        {          
            
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(livyUrl);

                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var byteArray = Encoding.ASCII.GetBytes(user + ":" + password);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
                                                

                HttpResponseMessage result = await client.DeleteAsync("/livy/sessions/"+ sessionId);

                if (result.IsSuccessStatusCode)
                {
                    

                }

            }

            await Task.CompletedTask;
        }

        public async Task<Statement> PostStatement(string sessionId, string statement)
        {

            var data = "{'code': '"+ statement + "'}";


            Statement response = null;
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(livyUrl);

                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var byteArray = Encoding.ASCII.GetBytes(user + ":" + password);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));


                JToken jt = JToken.Parse(data);
                string formattedJson = jt.ToString();


                HttpResponseMessage result = await client.PostAsync("/livy/sessions/" + sessionId +"/statements", new StringContent(formattedJson, Encoding.UTF8, "application/json"));

                if (result.IsSuccessStatusCode)
                {
                    response = await result.Content.ReadAsAsync<Statement>().ConfigureAwait(false);

                }
                else
                {
                    var s = await result.Content.ReadAsAsync<string>().ConfigureAwait(false);
                }

            }

            return response;

        }

        public async Task<Statements> GetStatementsResult(string sessionId)
        {

            Statements response = null;
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(livyUrl);

                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var byteArray = Encoding.ASCII.GetBytes(user + ":" + password);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));

                              

                HttpResponseMessage result = await client.GetAsync("/livy/sessions/" + sessionId + "/statements");

                if (result.IsSuccessStatusCode)
                {
                    response = await result.Content.ReadAsAsync<Statements>().ConfigureAwait(false);

                }

            }

            return response;

        }

        public async Task<Log> GetSessionLog(string sessionId)
        {

            Log response = null;
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(livyUrl);

                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var byteArray = Encoding.ASCII.GetBytes(user + ":" + password);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));



                HttpResponseMessage result = await client.GetAsync("/livy/sessions/" + sessionId + "/logs");

                if (result.IsSuccessStatusCode)
                {
                    response = await result.Content.ReadAsAsync<Log>().ConfigureAwait(false);

                }

            }

            return response;

        }

    }
}
