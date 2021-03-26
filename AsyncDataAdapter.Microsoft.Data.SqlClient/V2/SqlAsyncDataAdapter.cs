using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter
{
    using ProxyDbDataAdapterForSqlClient = ProxyDbDataAdapter<
        SqlDataAdapter,
        SqlConnection,
        SqlCommand,
        SqlDataReader
    >;

    public sealed class SqlAsyncDbDataAdapter2 : ProxyDbDataAdapterForSqlClient
    {
        public SqlAsyncDbDataAdapter2()
            : base( batchingAdapter: null, subject: new SqlDataAdapter() )
        {
        }

        public SqlAsyncDbDataAdapter2( SqlCommand selectCommand )
            : base( batchingAdapter: null, subject: new SqlDataAdapter( selectCommand ) )
        {
        }

        public SqlAsyncDbDataAdapter2( String selectCommandText, SqlConnection connection )
            : base( batchingAdapter: null, subject: new SqlDataAdapter( selectCommandText, connection ) )
        {
        }

        public SqlAsyncDbDataAdapter2( String selectCommandText, String connectionString )
            : base( batchingAdapter: null, subject: new SqlDataAdapter( selectCommandText, connectionString ) )
        {
        }
    }
}
