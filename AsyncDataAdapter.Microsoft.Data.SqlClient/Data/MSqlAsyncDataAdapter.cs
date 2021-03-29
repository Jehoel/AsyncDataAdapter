using System;
using System.Data.Common;

using Microsoft.Data.SqlClient;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    using ProxyDbDataAdapterForSqlClient = ProxyDbDataAdapter<
        SqlDataAdapter,
        SqlConnection,
        SqlCommand,
        SqlDataReader
    >;

    /// <summary>For use with <see cref="Microsoft.Data.SqlClient.SqlDataAdapter"/> (<c>Microsoft.Data.SqlClient</c>, not <c>System.Data.SqlClient</c>).</summary>
    public sealed class MSqlAsyncDbDataAdapter : ProxyDbDataAdapterForSqlClient
    {
        public MSqlAsyncDbDataAdapter()
            : this( original: new SqlDataAdapter() )
        {
        }

        public MSqlAsyncDbDataAdapter( SqlCommand selectCommand )
            : this( original: new SqlDataAdapter( selectCommand ) )
        {
        }

        public MSqlAsyncDbDataAdapter( String selectCommandText, SqlConnection connection )
            : this( original: new SqlDataAdapter( selectCommandText, connection ) )
        {
        }

        public MSqlAsyncDbDataAdapter( String selectCommandText, String connectionString )
            : this( original: new SqlDataAdapter( selectCommandText, connectionString ) )
        {
        }

        //

        private MSqlAsyncDbDataAdapter( SqlDataAdapter original )
            : this( original: original, batching: new BatchingMSqlDataAdapter( original ) )
        {

        }

        private MSqlAsyncDbDataAdapter( SqlDataAdapter original, BatchingMSqlDataAdapter batching )
            : base( batchingAdapter: batching, subject: original )
        {
        }

        protected override DbCommandBuilder CreateCommandBuilder()
        {
            return new SqlCommandBuilder( this.Subject );
        }
    }
}
