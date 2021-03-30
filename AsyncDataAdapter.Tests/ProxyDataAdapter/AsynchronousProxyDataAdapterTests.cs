using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests
{
    /// <summary>These tests demonstrate that <see cref="ProxyDbDataAdapter{TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader}"/>'s async methods are truly async with no calls into synchronous code-paths.</summary>
    public class AsynchronousProxyDataAdapterTests
    {
        [Test]
        public async Task Proxy_FillAsync_should_work_identically_to_DbDataReader_Fill()
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            // Part 1: Use proxy
            DataSet dataSetFromProxy;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AwaitAsync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    await connection.OpenAsync();

                    using( BatchingFakeProxiedDbDataAdapter adpt = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                    {
                        dataSetFromProxy = new DataSet();

                        // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                        Int32 rowsInFirstTable = await adpt.FillAsync( dataSetFromProxy );
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
            DataTableMethods.DataSetEquals( dataSetFromProxy, dataSetFromReal, out String diffs ).ShouldBeTrue( customMessage: diffs );
        }

        [Test]
        [TestCase( SchemaType.Mapped )]
        [TestCase( SchemaType.Source )]
        public async Task Proxy_FillSchemaAsync_should_work_identically_to_FillSchema_SchemaType( SchemaType schemaType )
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            // Part 1: Use proxy
            DataTable[] whatIsThisFromProxy;
            DataSet schemaFromProxy;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AwaitAsync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    await connection.OpenAsync();

                    using( BatchingFakeProxiedDbDataAdapter adpt = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                    {
                        schemaFromProxy = new DataSet();

                        whatIsThisFromProxy = await adpt.FillSchemaAsync( schemaFromProxy, schemaType );
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
            DataTableMethods.DataSetEquals( schemaFromProxy, schemaFromReal, out String diffs ).ShouldBeTrue( customMessage: diffs );
        }

        [Test]
        public async Task Proxy_UpdateAsync_should_work_identically_to_Update()
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            // Part 1: Use proxy
            DataSet dataSetFromProxy;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AwaitAsync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    await connection.OpenAsync();

                    using( BatchingFakeProxiedDbDataAdapter adapter = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                    using( DbCommandBuilder cmdBuilder = await adapter.CreateCommandBuilderAsync().ConfigureAwait(false) )
                    {
                        dataSetFromProxy = new DataSet();

                        // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                        Int32 rowsInFirstTable = await adapter.FillAsync( dataSetFromProxy );
                        rowsInFirstTable.ShouldBe( 40 );

                        //

                        Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataSet( dataSetFromProxy );

                        //

                        adapter.UpdateCommand = (FakeDbCommand)cmdBuilder.GetUpdateCommand();
                        adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => 1; // HACK /* DataTableMethods.GetNonQueryResultRowCountValue( dataSetFromProxy, cmd, rowsModified ); */;

                        //

                        Int32 updatedRows = await adapter.UpdateAsync( dataSetFromProxy ); // updatedRows... in first table only?
//                      updatedRows.ShouldBe( rowsModified );
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

                    using( FakeDbDataAdapter adapter = new FakeDbDataAdapter( selectCommand ) )
                    using( FakeDbCommandBuilder cmdBuilder = adapter.CreateCommandBuilder() )
                    {
                        dataSetFromReal = new DataSet();

                        // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                        Int32 rowsInFirstTable = adapter.Fill( dataSetFromReal );
                        rowsInFirstTable.ShouldBe( 40 );

                        //

                        Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataSet( dataSetFromReal );

                        //

                        adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                        adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => 1; // HACK /* DataTableMethods.GetNonQueryResultRowCountValue( dataSetFromProxy, cmd, rowsModified ); */;

                        //

                        Int32 updatedRows = adapter.Update( dataSetFromReal ); // updatedRows... in first table only?
//                      updatedRows.ShouldBe( rowsModified );
                    }
                }
            }

            // Assert equality:
            DataTableMethods.DataSetEquals( dataSetFromProxy, dataSetFromReal, out String diffs ).ShouldBeTrue( customMessage: diffs );
        }
    }
}
