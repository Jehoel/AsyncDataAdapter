using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter
{
    public sealed class SqlAsyncDbDataAdapter2 : ProxyDbDataAdapter
        <
            SqlDataAdapter,
            SqlConnection,
            SqlCommand,
            SqlDataReader
        >
    {
        public SqlAsyncDbDataAdapter2()
            : base( subject: new SqlDataAdapter() )
        {
        }

        public SqlAsyncDbDataAdapter2( SqlCommand selectCommand )
            : base( subject: new SqlDataAdapter( selectCommand ) )
        {
        }

        public SqlAsyncDbDataAdapter2( String selectCommandText, SqlConnection connection )
            : base( subject: new SqlDataAdapter( selectCommandText, connection ) )
        {
        }

        public SqlAsyncDbDataAdapter2( String selectCommandText, String connectionString )
            : base( subject: new SqlDataAdapter( selectCommandText, connectionString ) )
        {
        }
    }
}
