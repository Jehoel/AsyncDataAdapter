using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests.Big3
{
    public class Fill5Test : SingleMethodTest<DataTable[]>
    {
        protected override DataTable[] RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataTable[] tables = new DataTable[5];

            Int32 rowsInFirstTable = adapter.Fill5( startRecord: 5, maxRecords: 10, dataTables: tables );
            rowsInFirstTable.ShouldBe( 40 );

            return tables;
        }

        protected override DataTable[] RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable[] tables = new DataTable[5];

            Int32 rowsInFirstTable = adapter.Fill5( startRecord: 5, maxRecords: 10, dataTables: tables );
            rowsInFirstTable.ShouldBe( 40 );

            return tables;
        }

        protected override async Task<DataTable[]> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable[] tables = new DataTable[5];

            Int32 rowsInFirstTable = await adapter.Fill5Async( startRecord: 5, maxRecords: 10, dataTables: tables );
            rowsInFirstTable.ShouldBe( 40 );

            return tables;
        }

        protected override async Task<DataTable[]> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataTable[] tables = new DataTable[5];

            Int32 rowsInFirstTable = await adapter.Fill5Async( startRecord: 5, maxRecords: 10, dataTables: tables );
            rowsInFirstTable.ShouldBe( 40 );

            return tables;
        }

        protected override void AssertResult(DataTable[] dbSynchronous, DataTable[] dbProxied, DataTable[] dbProxiedAsync, DataTable[] dbBatchingProxiedAsync)
        {
            DataTableMethods.DataTablesEquals( dbSynchronous, dbProxied ).ShouldBeTrue();
            DataTableMethods.DataTablesEquals( dbSynchronous, dbProxiedAsync ).ShouldBeTrue();
            DataTableMethods.DataTablesEquals( dbSynchronous, dbBatchingProxiedAsync ).ShouldBeTrue();
        }
    }
}
