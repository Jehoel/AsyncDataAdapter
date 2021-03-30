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

    public static class SqlClientExtensions
    {
        /// <summary>Creates a new <see cref="SqlAsyncDbDataAdapter"/> using <paramref name="selectCommand"/> (the <c><see langword="this"/></c> extension method subject) as the <see cref="DbDataAdapter.SelectCommand"/>. Note that the <paramref name="selectCommand"/>'s <see cref="SqlCommand.Connection"/> property MUST be non-null. The connection does not need to be in an Open state yet, however.</summary>
        /// <param name="selectCommand">Required. Cannot be null. Must have a valid non-null <see cref="SqlCommand.Connection"/> set.</param>
        public static SqlAsyncDbDataAdapter CreateAsyncAdapter( this SqlCommand selectCommand )
        {
            if (selectCommand is null) throw new ArgumentNullException(nameof(selectCommand));
            
            if( selectCommand.Connection is null ) throw new ArgumentException( message: "The Connection property must be set.", paramName: nameof(selectCommand) );

            return new SqlAsyncDbDataAdapter( selectCommand );
        }
    }
}
