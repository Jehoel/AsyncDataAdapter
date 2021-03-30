using System;
using System.Data.Common;
using System.Data.SqlClient;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    using ProxyDbDataAdapterForSqlClient = ProxyDbDataAdapter<
        SqlDataAdapter,
        SqlConnection,
        SqlCommand,
        SqlDataReader
    >;

    /// <summary>For use with <see cref="System.Data.SqlClient.SqlDataAdapter"/> (<c>System.Data.SqlClient</c>, not <c>Microsoft.Data.SqlClient</c>).</summary>
    public sealed class SqlAsyncDbDataAdapter : ProxyDbDataAdapterForSqlClient
    {
        public SqlAsyncDbDataAdapter()
            : this( original: new SqlDataAdapter() )
        {
        }

        public SqlAsyncDbDataAdapter( SqlCommand selectCommand )
            : this( original: new SqlDataAdapter( selectCommand ) )
        {
        }

        public SqlAsyncDbDataAdapter( String selectCommandText, SqlConnection connection )
            : this( original: new SqlDataAdapter( selectCommandText, connection ) )
        {
        }

        public SqlAsyncDbDataAdapter( String selectCommandText, String connectionString )
            : this( original: new SqlDataAdapter( selectCommandText, connectionString ) )
        {
        }

        //

        private SqlAsyncDbDataAdapter( SqlDataAdapter original )
            : this( original: original, batching: new BatchingSqlDataAdapter( original ) )
        {

        }

        private SqlAsyncDbDataAdapter( SqlDataAdapter original, BatchingSqlDataAdapter batching )
            : base( batchingAdapter: batching, subject: original )
        {
        }

        protected override DbCommandBuilder CreateCommandBuilder()
        {
            return new SqlCommandBuilder( this.Subject );
        }
    }
}
