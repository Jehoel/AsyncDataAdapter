using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests
{
    public class Fill2Test : SingleMethodTest<DataSet>
    {
        protected override DataSet RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rowsInFirstTable = adapter.Fill2( dataSet, srcTable: "Foobar" );
            rowsInFirstTable.ShouldBe( 40 );

            return dataSet;
        }

        protected override DataSet RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rowsInFirstTable = adapter.Fill2( dataSet, srcTable: "Foobar" );
            rowsInFirstTable.ShouldBe( 40 );

            return dataSet;
        }

        protected override async Task<DataSet> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rowsInFirstTable = await adapter.Fill2Async( dataSet, srcTable: "Foobar" );
            rowsInFirstTable.ShouldBe( 40 );

            return dataSet;
        }

        protected override async Task<DataSet> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rowsInFirstTable = await adapter.Fill2Async( dataSet, srcTable: "Foobar" );
            rowsInFirstTable.ShouldBe( 40 );

            return dataSet;
        }

        protected override void AssertResult(DataSet dbSynchronous, DataSet dbProxied, DataSet dbProxiedAsync, DataSet dbBatchingProxiedAsync)
        {
            throw new NotImplementedException();
        }
    }
}
