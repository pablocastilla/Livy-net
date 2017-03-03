using System.Threading.Tasks;

namespace Livy_net
{
    public interface ILivyClient
    {
        Task CloseSession(string sessionId);
        Task<Log> GetSessionLog(string sessionId);
        Task<Session> GetSessionState(string sessionId);
        Task<Statements> GetStatementsResult(string sessionId);
        Task<Session> OpenSession();
        Task<Statement> PostStatement(string sessionId, string statement);

        Task<Statement> GetStatementResult(string sessionId, string stamentId);
    }
}