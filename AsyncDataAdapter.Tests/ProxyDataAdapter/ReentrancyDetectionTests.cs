using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using NUnit.Framework;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests
{
    public class ReentrancyDetectionTests
    {
        private static void FiddleWithPropertiesAsFakeProxiedDbDataAdapter( FakeProxiedDbDataAdapter adapter )
        {
            {
                FakeDbCommand cmd = adapter.SelectCommand;
                adapter.SelectCommand = null;
                adapter.SelectCommand = cmd;
            }

            {
                FakeDbCommand cmd = adapter.InsertCommand;
                adapter.InsertCommand = null;
                adapter.InsertCommand = cmd;
            }

            {
                FakeDbCommand cmd = adapter.DeleteCommand;
                adapter.DeleteCommand = null;
                adapter.DeleteCommand = cmd;
            }

            {
                FakeDbCommand cmd = adapter.UpdateCommand;
                adapter.UpdateCommand = null;
                adapter.UpdateCommand = cmd;
            }
        }

        private static void FiddleWithPropertiesAsFakeDbDataAdapter( FakeDbDataAdapter adapter )
        {
            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.SelectCommand;
                adapter.SelectCommand = null;
                adapter.SelectCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.InsertCommand;
                adapter.InsertCommand = null;
                adapter.InsertCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.DeleteCommand;
                adapter.DeleteCommand = null;
                adapter.DeleteCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.UpdateCommand;
                adapter.UpdateCommand = null;
                adapter.UpdateCommand = cmd;
            }
        }

        private static void FiddleWithPropertiesAsDbDataAdapter( DbDataAdapter adapter )
        {
            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.SelectCommand;
                adapter.SelectCommand = null;
                adapter.SelectCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.InsertCommand;
                adapter.InsertCommand = null;
                adapter.InsertCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.DeleteCommand;
                adapter.DeleteCommand = null;
                adapter.DeleteCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.UpdateCommand;
                adapter.UpdateCommand = null;
                adapter.UpdateCommand = cmd;
            }
        }

        private static void FiddleWithPropertiesAsIDbDataAdapter( IDbDataAdapter adapter )
        {
            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.SelectCommand;
                adapter.SelectCommand = null;
                adapter.SelectCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.InsertCommand;
                adapter.InsertCommand = null;
                adapter.InsertCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.DeleteCommand;
                adapter.DeleteCommand = null;
                adapter.DeleteCommand = cmd;
            }

            {
                FakeDbCommand cmd = (FakeDbCommand)adapter.UpdateCommand;
                adapter.UpdateCommand = null;
                adapter.UpdateCommand = cmd;
            }
        }

        [Test]
        public void FakeDbDataAdapter_properties_should_not_have_infinite_loops_and_stack_overflows()
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1, tableCount: 2, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            // Test that .Dispose() works (DbDataAdapter clears mutable properties in its disposal method)
            using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
            using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
            using( FakeDbDataAdapter adapter = new FakeDbDataAdapter( selectCommand ) )
            {
            }

            using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
            using( FakeDbCommand selectCommand1 = connection.CreateCommand( testTables: randomDataSource ) )
            using( FakeDbCommand selectCommand2 = connection.CreateCommand( testTables: randomDataSource ) )
            using( FakeDbDataAdapter adapter = new FakeDbDataAdapter( selectCommand1 ) )
            {
                using( FakeProxiedDbDataAdapter prox = new FakeProxiedDbDataAdapter( selectCommand1 ) )
                {
                    FiddleWithPropertiesAsFakeProxiedDbDataAdapter( prox );

                    FiddleWithPropertiesAsFakeDbDataAdapter( prox );

                    FiddleWithPropertiesAsDbDataAdapter( prox );

                    FiddleWithPropertiesAsIDbDataAdapter( prox );
                }

                FiddleWithPropertiesAsFakeDbDataAdapter( adapter );

                FiddleWithPropertiesAsDbDataAdapter( adapter );

                FiddleWithPropertiesAsIDbDataAdapter( adapter );
            }
        }
    }
}
