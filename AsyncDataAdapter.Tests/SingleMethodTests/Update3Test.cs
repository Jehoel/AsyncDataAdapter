using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests
{
    using U3Pair = ValueTuple<DataTable,Int32>; // ( DataTable ds, Int32 rows )

    public class Update3Test : SingleMethodTest<U3Pair>
    {
        protected override U3Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            Int32 rows = adapter.Update3( dataTable );

            return ( dataTable, rows );
        }

        protected override U3Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            Int32 rows = adapter.Update3( dataTable );

            return ( dataTable, rows );
        }

        protected override async Task<U3Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            Int32 rows = await adapter.Update3Async( dataTable );

            return ( dataTable, rows );
        }

        protected override async Task<U3Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            Int32 rows = await adapter.Update3Async( dataTable );

            return ( dataTable, rows );
        }

        protected override void AssertResult( U3Pair dbSynchronous, U3Pair dbProxied, U3Pair dbProxiedAsync, U3Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }
}
