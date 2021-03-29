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
    using U1Pair = ValueTuple<DataSet,Dictionary<String,Int32>,Int32>;

    public class Update1Test : SingleMethodTest<U1Pair>
    {
        #warning TODO: Variations of `UpdateNTest` that use custom Table/Column Mappings.
        // TODO: Variations of `UpdateNTest` that use custom Table/Column Mappings.
        // This is documented here: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/dataadapter-datatable-and-datacolumn-mappings
        // This would then cover the `Table`, `Table1`, etc. naming issues.

        protected override U1Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            using( FakeDbCommandBuilder cmdBuilder = adapter.CreateCommandBuilder() )
            {
                DataSet dataSet = new DataSet();

                // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                Int32 rowsInFirstTable = adapter.Fill( dataSet );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataSet( dataSet );

                //
                adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( adapter, dataSet, cmd, rowsModified );

                Int32 updatedRows = adapter.Update1( dataSet ); // updatedRows... in first table only?
//              updatedRows.ShouldBe( rowsModified );

                return ( dataSet, rowsModified, updatedRows );
            }
        }

        protected override U1Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            using( FakeDbCommandBuilder cmdBuilder = new FakeDbCommandBuilder( adapter ) )
            {
                DataSet dataSet = new DataSet();

                Int32 rowsInFirstTable = adapter.Fill( dataSet );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataSet( dataSet );

                //
                adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( adapter, dataSet, cmd, rowsModified );

                Int32 updatedRows = adapter.Update1( dataSet ); // updatedRows... in first table only?
//              updatedRows.ShouldBe( rowsModified );

                return ( dataSet, rowsModified, updatedRows );
            }
        }

        protected override async Task<U1Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            using( DbCommandBuilder cmdBuilder = await adapter.CreateCommandBuilderAsync().ConfigureAwait(false) )
            {
                DataSet dataSet = new DataSet();

                Int32 rowsInFirstTable = await adapter.FillAsync( dataSet );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataSet( dataSet );

                //
                adapter.UpdateCommand = (FakeDbCommand)cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( adapter, dataSet, cmd, rowsModified );

                //

                Int32 updatedRows = await adapter.Update1Async( dataSet ); // updatedRows... in first table only?
//              updatedRows.ShouldBe( rowsModified );

                return ( dataSet, rowsModified, updatedRows );
            }
        }

        protected override async Task<U1Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            using( DbCommandBuilder cmdBuilder = await adapter.CreateCommandBuilderAsync().ConfigureAwait(false) )
            {
                DataSet dataSet = new DataSet();

                Int32 rowsInFirstTable = await adapter.FillAsync( dataSet );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataSet( dataSet );

                //
                adapter.UpdateCommand = (FakeDbCommand)cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( adapter, dataSet, cmd, rowsModified );

                //

                Int32 updatedRows = await adapter.Update1Async( dataSet ); // updatedRows... in first table only?
//              updatedRows.ShouldBe( rowsModified );

                return ( dataSet, rowsModified, updatedRows );
            }
        }

        protected override void AssertResult( U1Pair dbSynchronous, U1Pair dbProxied, U1Pair dbProxiedAsync, U1Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }
}
