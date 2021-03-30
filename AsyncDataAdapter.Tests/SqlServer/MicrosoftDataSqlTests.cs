using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.Tests.SqlServer
{
    public class MicrosoftDataSqlTests : BaseSqlDataAdapterTest<SqlConnection,SqlCommand,SqlDataAdapter,MSqlAsyncDbDataAdapter>
    {
        protected override SqlConnection CreateConnection( String connectionString )
        {
            return new SqlConnection( connectionString );
        }

        protected override SqlCommand CreateCommand(SqlConnection connection)
        {
            return connection.CreateCommand();
        }

        protected override SqlDataAdapter CreateDbAdapter( SqlCommand cmd )
        {
            return new SqlDataAdapter( cmd );
        }

        protected override MSqlAsyncDbDataAdapter CreateAsyncDbAdapter( SqlCommand cmd )
        {
            return new MSqlAsyncDbDataAdapter( cmd );
        }
    }
}
