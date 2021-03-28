using System;
using System.Collections.Generic;
using System.Data.Common;

namespace AsyncDataAdapter.Tests.FakeDb
{
    public class FakeDbProviderFactory : DbProviderFactory
    {
        public static FakeDbProviderFactory Instance { get; } = new FakeDbProviderFactory();

        private FakeDbProviderFactory()
        {
        }

        public override bool CanCreateDataSourceEnumerator => false;

        public override bool CanCreateCommandBuilder => true;

        public override bool CanCreateDataAdapter => true;

        public override DbCommand CreateCommand()
        {
            return new FakeDbCommand();
        }

        public FakeDbCommand CreateCommand( FakeDbConnection connection, List<TestTable> testTables, TimeSpan? executeDelay, TimeSpan? readDelay )
        {
            return new FakeDbCommand( connection: connection, testTables: testTables, executeDelay: executeDelay, readDelay: readDelay )
            {
                AsyncMode = connection.AsyncMode
            };
        }

        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new FakeDbCommandBuilder();
        }

        public override DbConnection CreateConnection()
        {
            // Make this one fail every time, so we can be sure `CreateConnection` is never used without us knowing!
            // ...or just fail?
            throw new NotSupportedException();

            //return new FakeDbConnection( asyncMode: AsyncMode.None, openDelayMS: -1, executeDelayMS: -1, readDelayMS: -1 );
        }

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
//          return new FakeDbConnectionStringBuilder();
            return base.CreateConnectionStringBuilder();
        }

        public override DbDataAdapter CreateDataAdapter()
        {
            return new FakeDbDataAdapter();
        }

        public override DbDataSourceEnumerator CreateDataSourceEnumerator()
        {
//          return new FakeDbDataSourceEnumerator();
            return base.CreateDataSourceEnumerator();
        }

        public override DbParameter CreateParameter()
        {
            return new FakeDbParameter();
        }
    }
}
