using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests.Big3
{
    using U1Pair = ValueTuple<DataSet,Int32>; // ( DataSet ds, Int32 rows )

    public class Update1Test : SingleMethodTest<U1Pair>
    {
        protected override U1Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rows = adapter.Update1( dataSet );

            return ( dataSet, rows );
        }

        protected override U1Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rows = adapter.Update1( dataSet );

            return ( dataSet, rows );
        }

        protected override async Task<U1Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rows = await adapter.Update1Async( dataSet );

            return ( dataSet, rows );
        }

        protected override async Task<U1Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rows = await adapter.Update1Async( dataSet );

            return ( dataSet, rows );
        }

        protected override void AssertResult( U1Pair dbSynchronous, U1Pair dbProxied, U1Pair dbProxiedAsync, U1Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }
}
