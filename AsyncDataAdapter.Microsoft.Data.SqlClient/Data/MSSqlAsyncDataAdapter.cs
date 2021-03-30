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
    public sealed class MSSqlAsyncDbDataAdapter : ProxyDbDataAdapterForSqlClient
    {
        public MSSqlAsyncDbDataAdapter()
            : this( original: new SqlDataAdapter() )
        {
        }

        public MSSqlAsyncDbDataAdapter( SqlCommand selectCommand )
            : this( original: new SqlDataAdapter( selectCommand ) )
        {
        }

        public MSSqlAsyncDbDataAdapter( String selectCommandText, SqlConnection connection )
            : this( original: new SqlDataAdapter( selectCommandText, connection ) )
        {
        }

        public MSSqlAsyncDbDataAdapter( String selectCommandText, String connectionString )
            : this( original: new SqlDataAdapter( selectCommandText, connectionString ) )
        {
        }

        //

        private MSSqlAsyncDbDataAdapter( SqlDataAdapter original )
            : this( original: original, batching: new BatchingMSqlDataAdapter( original ) )
        {

        }

        private MSSqlAsyncDbDataAdapter( SqlDataAdapter original, BatchingMSqlDataAdapter batching )
            : base( batchingAdapter: batching, subject: original )
        {
        }

        protected override DbCommandBuilder CreateCommandBuilder()
        {
            return new SqlCommandBuilder( this.Subject );
        }
    }

    public static class MSSqlClientExtensions
    {
        /// <summary>Creates a new <see cref="MSSqlAsyncDbDataAdapter"/> using <paramref name="selectCommand"/> (the <c><see langword="this"/></c> extension method subject) as the <see cref="DbDataAdapter.SelectCommand"/>. Note that the <paramref name="selectCommand"/>'s <see cref="SqlCommand.Connection"/> property MUST be non-null. The connection does not need to be in an Open state yet, however.</summary>
        /// <param name="selectCommand">Required. Cannot be null. Must have a valid non-null <see cref="SqlCommand.Connection"/> set.</param>
        public static MSSqlAsyncDbDataAdapter CreateAsyncAdapter( this SqlCommand selectCommand )
        {
            if (selectCommand is null) throw new ArgumentNullException(nameof(selectCommand));
            
            if( selectCommand.Connection is null ) throw new ArgumentException( message: "The Connection property must be set.", paramName: nameof(selectCommand) );

            return new MSSqlAsyncDbDataAdapter( selectCommand );
        }
    }
}
