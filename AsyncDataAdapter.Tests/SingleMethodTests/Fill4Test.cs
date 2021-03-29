using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests.Big3
{
    public class Fill4Test : SingleMethodTest<DataTable>
    {
        protected override DataTable RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            Int32 rowsInFirstTable = adapter.Fill4( dataTable: dataTable );
            rowsInFirstTable.ShouldBe( 40 );

            return dataTable;
        }

        protected override DataTable RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            Int32 rowsInFirstTable = adapter.Fill4( dataTable: dataTable );
            rowsInFirstTable.ShouldBe( 40 );

            return dataTable;
        }

        protected override async Task<DataTable> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            Int32 rowsInFirstTable = await adapter.Fill4Async( dataTable: dataTable );
            rowsInFirstTable.ShouldBe( 40 );

            return dataTable;
        }

        protected override async Task<DataTable> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            Int32 rowsInFirstTable = await adapter.Fill4Async( dataTable );
            rowsInFirstTable.ShouldBe( 40 );

            return dataTable;
        }

        protected override void AssertResult(DataTable dbSynchronous, DataTable dbProxied, DataTable dbProxiedAsync, DataTable dbBatchingProxiedAsync)
        {
            DataTableMethods.DataTableEquals( dbSynchronous, dbProxied             , out Int32? diffs1 ).ShouldBeTrue( customMessage: "First different row at index: " + diffs1 );
            DataTableMethods.DataTableEquals( dbSynchronous, dbProxiedAsync        , out Int32? diffs2 ).ShouldBeTrue( customMessage: "First different row at index: " + diffs2 );
            DataTableMethods.DataTableEquals( dbSynchronous, dbBatchingProxiedAsync, out Int32? diffs3 ).ShouldBeTrue( customMessage: "First different row at index: " + diffs3 );
        }
    }
}
