using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

using System.Data.SqlClient;

namespace AsyncDataAdapter.Tests.SqlServer
{
    public class SystemDataSqlTests : BaseSqlDataAdapterTest<SqlConnection,SqlCommand,SqlDataAdapter,SqlAsyncDbDataAdapter>
    {
        protected override SqlConnection CreateConnection( String connectionString )
        {
            return new SqlConnection( connectionString );
        }

        protected override SqlCommand CreateCommand( SqlConnection connection )
        {
            return connection.CreateCommand();
        }

        protected override SqlDataAdapter CreateDbAdapter( SqlCommand cmd )
        {
            return new SqlDataAdapter( cmd );
        }

        protected override SqlAsyncDbDataAdapter CreateAsyncDbAdapter( SqlCommand cmd )
        {
            return new SqlAsyncDbDataAdapter( cmd );
        }
    }
}
