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
    using U4Pair = ValueTuple<DataSet,Dictionary<String,Int32>,Int32>;

    public class Update4Test : SingleMethodTest<U4Pair>
    {
        protected override U4Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
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

                Int32 updatedRows = adapter.Update4( dataSet, srcTable: "RandomDataTable_2" );
//              updatedRows.ShouldBe( rowsModified );

                return ( dataSet, rowsModified, updatedRows );
            }
        }

        protected override U4Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            using( FakeDbCommandBuilder cmdBuilder = new FakeDbCommandBuilder( adapter ) )
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

                Int32 updatedRows = adapter.Update4( dataSet, srcTable: "RandomDataTable_2" );
//              updatedRows.ShouldBe( rowsModified );

                return ( dataSet, rowsModified, updatedRows );
            }
        }

        protected override async Task<U4Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            using( DbCommandBuilder<FakeDbCommand> cmdBuilder = await adapter.CreateCommandBuilderAsync().ConfigureAwait(false) )
            {
                DataSet dataSet = new DataSet();

                // `.Fill` returns the number of rows in the first table, not any subsequent tables. Yes, that's silly.
                Int32 rowsInFirstTable = await adapter.FillAsync( dataSet );
                rowsInFirstTable.ShouldBe( 40 );

                //
                Dictionary<String,Int32> rowsModified = DataTableMethods.MutateDataSet( dataSet );

                //
                adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
                adapter.UpdateCommand.NonQueryResultRowCountValue = ( cmd ) => DataTableMethods.GetNonQueryResultRowCountValue( adapter, dataSet, cmd, rowsModified );

                Int32 updatedRows = await adapter.Update4Async( dataSet, srcTable: "RandomDataTable_2" );
//              updatedRows.ShouldBe( rowsModified );

                return ( dataSet, rowsModified, updatedRows );
            }
        }

        protected override async Task<U4Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            using( DbCommandBuilder<FakeDbCommand> cmdBuilder = await adapter.CreateCommandBuilderAsync().ConfigureAwait(false) )
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

                Int32 updatedRows = await adapter.Update4Async( dataSet, srcTable: "RandomDataTable_2" );
//              updatedRows.ShouldBe( rowsModified );

                return ( dataSet, rowsModified, updatedRows );
            }
        }

        protected override void AssertResult( U4Pair dbSynchronous, U4Pair dbProxied, U4Pair dbProxiedAsync, U4Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }
}
