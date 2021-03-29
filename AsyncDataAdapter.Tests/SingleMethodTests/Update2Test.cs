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
    using U2Pair = ValueTuple<DataRow[],Dictionary<String,Int32>,Int32>;

    public class Update2Test : SingleMethodTest<U2Pair>
    {
        protected override U2Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
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
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( cmd, rowsModified );

                DataRow[] rows = dataTable.Rows.Cast<DataRow>().ToArray();

                Int32 updatedRows = adapter.Update2( rows );
//              updatedRows.ShouldBe( rowsModified );

                return ( rows, rowsModified, updatedRows );
            }
        }

        protected override U2Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
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
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( cmd, rowsModified );

                DataRow[] rows = dataTable.Rows.Cast<DataRow>().ToArray();

                Int32 updatedRows = adapter.Update2( rows );
//              updatedRows.ShouldBe( rowsModified );

                return ( rows, rowsModified, updatedRows );
            }
        }

        protected override async Task<U2Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
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
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( cmd, rowsModified );

                DataRow[] rows = dataTable.Rows.Cast<DataRow>().ToArray();

                Int32 updatedRows = await adapter.Update2Async( rows );
//              updatedRows.ShouldBe( rowsModified );

                return ( rows, rowsModified, updatedRows );
            }
        }

        protected override async Task<U2Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
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
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( cmd, rowsModified );

                DataRow[] rows = dataTable.Rows.Cast<DataRow>().ToArray();

                Int32 updatedRows = await adapter.Update2Async( rows );
//              updatedRows.ShouldBe( rowsModified );

                return ( rows, rowsModified, updatedRows );
            }
        }

        protected override void AssertResult( U2Pair dbSynchronous, U2Pair dbProxied, U2Pair dbProxiedAsync, U2Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }
}
