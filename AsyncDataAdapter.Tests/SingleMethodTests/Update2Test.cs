using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests
{
    using U2Pair = ValueTuple<DataRow[],Int32>; // ( DataRow[] dataRows, Int32 rows )

    public class Update2Test : SingleMethodTest<U2Pair>
    {
        protected override U2Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataRow[] dataRows = new DataRow[10];

            Int32 rows = adapter.Update2( dataRows );

            return ( dataRows, rows );
        }

        protected override U2Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataRow[] dataRows = new DataRow[10];

            Int32 rows = adapter.Update2( dataRows );

            return ( dataRows, rows );
        }

        protected override async Task<U2Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataRow[] dataRows = new DataRow[10];

            Int32 rows = await adapter.Update2Async( dataRows );

            return ( dataRows, rows );
        }

        protected override async Task<U2Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataRow[] dataRows = new DataRow[10];

            Int32 rows = await adapter.Update2Async( dataRows );

            return ( dataRows, rows );
        }

        protected override void AssertResult( U2Pair dbSynchronous, U2Pair dbProxied, U2Pair dbProxiedAsync, U2Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }
}
