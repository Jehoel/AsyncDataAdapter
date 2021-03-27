using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

using NUnit.Framework;

using Shouldly;

namespace AsyncDataAdapter.Tests
{
    // The purpose of these tests is to ensure that <see cref="ProxyDbDataAdapter{TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader}"/> is correctly implemented by using a fake DataAdapter.

    /// <summary>These tests demonstrate that <see cref="ProxyDbDataAdapter{TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader}"/> forwards all synchronous calls to the underlying <see cref="DbDataAdapter"/>.</summary>
    public class SynchronousProxyDataAdapterTests
    {
        [Test]
        public void FakeDbDataAdapter_properties_should_not_have_infinite_loops_and_stack_overflows()
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1, tableCount: 2, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            try
            {
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
                    FakeDbCommand cmd1 = (FakeDbCommand)adapter.SelectCommand;
                    adapter.SelectCommand = null;
                    adapter.SelectCommand = cmd1;
                    adapter.SelectCommand = selectCommand2;
                }
            }
            catch( Exception ex )
            {
                Assert.Fail( ex.ToString() );
            }
        }

        #warning TODO: Test every overload of Fill, FillSchema, and Update!
        // TODO: Test every overload of Fill, FillSchema, and Update!

        [Test]
        public void Proxy_Fill_should_work_identically_to_DbDataReader_Fill()
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            // Part 1: Use proxy
            DataSet dataSetFromProxy;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    connection.Open();

                    using( BatchingFakeProxiedDbDataAdapter adpt = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                    {
                        dataSetFromProxy = new DataSet();

                        // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                        Int32 rowsInFirstTable = adpt.Fill( dataSetFromProxy );
                        rowsInFirstTable.ShouldBe( 40 );
                    }
                }
            }

            // Part 2: Use real
            DataSet dataSetFromReal;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    connection.Open();

                    using( FakeDbDataAdapter adpt = new FakeDbDataAdapter( selectCommand ) )
                    {
                        dataSetFromReal = new DataSet();

                        Int32 rowsInFirstTable = adpt.Fill( dataSetFromReal );
                        rowsInFirstTable.ShouldBe( 40 );
                    }
                }
            }

            // Assert equality:
            DataTableEquality.DataSetEquals( dataSetFromProxy, dataSetFromReal ).ShouldBeTrue();
        }

        [Test]
        [TestCase( SchemaType.Mapped )]
        [TestCase( SchemaType.Source )]
        public void ProxyFillSchema_should_work_identically_to_FillSchema_SchemaType( SchemaType schemaType )
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            // Part 1: Use proxy
            DataTable[] whatIsThisFromProxy;
            DataSet schemaFromProxy;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    connection.Open();

                    using( BatchingFakeProxiedDbDataAdapter adpt = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                    {
                        schemaFromProxy = new DataSet();

                        whatIsThisFromProxy = adpt.FillSchema( schemaFromProxy, schemaType );
                    }
                }
            }

            // Part 2: Use real
            DataTable[] whatIsThisFromReal;
            DataSet schemaFromReal;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    connection.Open();

                    using( FakeDbDataAdapter adpt = new FakeDbDataAdapter( selectCommand ) )
                    {
                        schemaFromReal = new DataSet();

                        whatIsThisFromReal = adpt.FillSchema( schemaFromReal, schemaType );
                    }
                }
            }

            // Assert equality:
            DataTableEquality.DataSetEquals( schemaFromProxy, schemaFromReal ).ShouldBeTrue();
        }

        [Test]
        [TestCase( SchemaType.Mapped )]
        [TestCase( SchemaType.Source )]
        public void ProxyUpdate_should_work_identically_to_Update( SchemaType schemaType )
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            // Part 1: Use proxy
            DataSet dataSetFromProxy;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    connection.Open();

                    using( BatchingFakeProxiedDbDataAdapter adpt = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                    using( FakeDbCommandBuilder cmdBuilder = adpt.CreateCommandBuilder() )
                    {
                        dataSetFromProxy = new DataSet();

                        // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                        Int32 rowsInFirstTable = adpt.Fill( dataSetFromProxy );
                        rowsInFirstTable.ShouldBe( 40 );

                        //

                        MutateDataSet( dataSetFromProxy );

                        //
                        adpt.UpdateCommand = cmdBuilder.GetUpdateCommand();

                        Int32 updatedRows = adpt.Update( dataSetFromProxy ); // updatedRows... in first table only?
                    }
                }
            }

            // Part 2: Use real
            DataSet dataSetFromReal;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    connection.Open();

                    using( FakeDbDataAdapter adpt = new FakeDbDataAdapter( selectCommand ) )
                    using( FakeDbCommandBuilder cmdBuilder = adpt.CreateCommandBuilder() )
                    {
                        dataSetFromReal = new DataSet();

                        // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                        Int32 rowsInFirstTable = adpt.Fill( dataSetFromReal );
                        rowsInFirstTable.ShouldBe( 40 );

                        //

                        MutateDataSet( dataSetFromReal );

                        //
                        adpt.UpdateCommand = cmdBuilder.GetUpdateCommand();

                        Int32 updatedRows = adpt.Update( dataSetFromReal ); // updatedRows... in first table only?
                    }
                }
            }

            // Assert equality:
            DataTableEquality.DataSetEquals( dataSetFromProxy, dataSetFromReal ).ShouldBeTrue();
        }

        private static void MutateDataSet( DataSet dataSet )
        {

        }
    }
}
