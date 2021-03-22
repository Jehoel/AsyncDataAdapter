using System;
using System.Collections;
using System.Threading;
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

        int GetParameterCount(int commandIndex);

        SqlParameter GetParameter(int commandIndex, int parameterIndex);

        bool GetBatchedAffected(int commandIdentifier, out int recordsAffected, out Exception error);

        #region Private

        /// <summary>Actual element type is <c>Microsoft.Data.SqlClient.SqlCommandSet.LocalCommand</c>.</summary>
        IList CommandList { get; }

        SqlCommand BatchCommand { get; }

        #endregion

        Task<int> ExecuteNonQueryAsync( CancellationToken cancellationToken );
    }
}
