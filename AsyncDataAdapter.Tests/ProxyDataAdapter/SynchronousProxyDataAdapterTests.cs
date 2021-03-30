using System;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;
using System.Data.Common;

namespace AsyncDataAdapter.Tests
{
    // The purpose of these tests is to ensure that <see cref="ProxyDbDataAdapter{TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader}"/> is correctly implemented by using a fake DataAdapter.

    /// <summary>These tests demonstrate that <see cref="ProxyDbDataAdapter{TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader}"/> forwards all synchronous calls to the underlying <see cref="DbDataAdapter"/>.</summary>
    public class SynchronousProxyDataAdapterTests
    {
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
            DataTableMethods.DataSetEquals( dataSetFromProxy, dataSetFromReal, out String diffs ).ShouldBeTrue( customMessage: diffs );
        }

        [Test]
        [TestCase( SchemaType.Mapped )]
        [TestCase( SchemaType.Source )]
        public void Proxy_FillSchema_should_work_identically_to_FillSchema_SchemaType( SchemaType schemaType )
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
            DataTableMethods.DataSetEquals( schemaFromProxy, schemaFromReal, out String diffs ).ShouldBeTrue( customMessage: diffs );
        }

        [Test]
        public void Proxy_Update_should_work_identically_to_Update()
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            // TODO: Multiple table UPDATE support is... complicated: https://stackoverflow.com/questions/16218856/how-to-update-two-tables-with-one-dataset

            // Part 1: Use proxy
            DataSet dataSetFromProxy;
            {
                using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
                using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
                {
                    connection.Open();

                    using( BatchingFakeProxiedDbDataAdapter adapter = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                    using( FakeDbCommandBuilder cmdBuilder = new FakeDbCommandBuilder( adapter ) )
                    {
                        dataSetFromProxy = new DataSet();

                        // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                        Int32 rowsInFirstTable = adapter.Fill( dataSetFromProxy );
                        rowsInFirstTable.ShouldBe( 40 );

                        //

                        Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataSet( dataSetFromProxy );

                        //
                        adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                        adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetUpdateStatementNonQueryResultRowCountValue( expectedTableName: "TODO", adapter, dataSetFromProxy, cmd, rowsModified );

                        Int32 updatedRows = adapter.Update( dataSetFromProxy ); // updatedRows... in first table only?
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
                        adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetUpdateStatementNonQueryResultRowCountValue( expectedTableName: "TODO", adapter, dataSetFromReal, cmd, rowsModified );

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
