using System;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.SqlClient
{
    public interface ISqlCommandSet : IDisposable
    {
        SqlConnection Connection { get; set; }
        
        SqlTransaction Transaction { get; set; }
        
        int CommandTimeout { get; set; }

        int CommandCount { get; }

        void Append(SqlCommand cmd);

        void Clear();

        int ExecuteNonQuery();

        Task<int> ExecuteNonQueryAsync();

        int GetParameterCount(int commandIndex);

        SqlParameter GetParameter(int commandIndex, int parameterIndex);

        bool GetBatchedAffected(int commandIdentifier, out int recordsAffected, out Exception error);
    }
}
