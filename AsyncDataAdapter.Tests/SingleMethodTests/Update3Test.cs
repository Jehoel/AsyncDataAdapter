using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests.Big3
{
    using U3Pair = ValueTuple<DataTable,Dictionary<String,Int32>,Int32>;

    public class Update3Test : SingleMethodTest<U3Pair>
    {
        protected override U3Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            using( FakeDbCommandBuilder cmdBuilder = adapter.CreateCommandBuilder() )
            {
                DataTable dataTable = new DataTable();

                // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                Int32 rowsInFirstTable = adapter.Fill( dataTable );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataTable( dataTable );

                //
                adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetUpdateStatementNonQueryResultRowCountValue( expectedTableName: "TODO", adapter, dataTable, cmd, rowsModified );

                Int32 updatedRows = adapter.Update3( dataTable );
//              updatedRows.ShouldBe( rowsModified );

                return ( dataTable, rowsModified, updatedRows );
            }
        }

        protected override U3Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            using( FakeDbCommandBuilder cmdBuilder = new FakeDbCommandBuilder( adapter ) )
            {
                DataTable dataTable = new DataTable();

                // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                Int32 rowsInFirstTable = adapter.Fill( dataTable );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataTable( dataTable );

                //
                adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetUpdateStatementNonQueryResultRowCountValue( expectedTableName: "TODO", adapter, dataTable, cmd, rowsModified );

                Int32 updatedRows = adapter.Update3( dataTable );
//              updatedRows.ShouldBe( rowsModified );

                return ( dataTable, rowsModified, updatedRows );
            }
        }

        protected override async Task<U3Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            using( DbCommandBuilder<FakeDbCommand> cmdBuilder = await adapter.CreateCommandBuilderAsync().ConfigureAwait(false) )
            {
                DataTable dataTable = new DataTable();

                // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                Int32 rowsInFirstTable = await adapter.FillAsync( dataTable );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataTable( dataTable );

                //
                adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetUpdateStatementNonQueryResultRowCountValue( expectedTableName: "TODO", adapter, dataTable, cmd, rowsModified );

                Int32 updatedRows = await adapter.Update3Async( dataTable );
//              updatedRows.ShouldBe( rowsModified );

                return ( dataTable, rowsModified, updatedRows );
            }
        }

        protected override async Task<U3Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            using( DbCommandBuilder<FakeDbCommand> cmdBuilder = await adapter.CreateCommandBuilderAsync().ConfigureAwait(false) )
            {
                DataTable dataTable = new DataTable();

                // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                Int32 rowsInFirstTable = adapter.Fill( dataTable );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataTable( dataTable );

                //
                adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetUpdateStatementNonQueryResultRowCountValue( expectedTableName: "TODO", adapter, dataTable, cmd, rowsModified );

                DataRow[] rows = dataTable.Rows.Cast<DataRow>().ToArray();

                Int32 updatedRows = await adapter.Update3Async( dataTable );
//              updatedRows.ShouldBe( rowsModified );

                return ( dataTable, rowsModified, updatedRows );
            }
        }

        protected override void AssertResult( U3Pair dbSynchronous, U3Pair dbProxied, U3Pair dbProxiedAsync, U3Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }
}
