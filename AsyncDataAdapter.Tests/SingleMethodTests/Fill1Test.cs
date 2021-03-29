using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;
using NUnit.Framework;

namespace AsyncDataAdapter.Tests.Big3
{
    public class Fill1Test : SingleMethodTest<DataSet>
    {
        protected override DataSet RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rowsInFirstTable = adapter.Fill1( dataSet );
            rowsInFirstTable.ShouldBe( 40 );

            return dataSet;
        }

        protected override DataSet RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rowsInFirstTable = adapter.Fill1( dataSet );
            rowsInFirstTable.ShouldBe( 40 );

            return dataSet;
        }

        protected override async Task<DataSet> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rowsInFirstTable = await adapter.Fill1Async( dataSet );
            rowsInFirstTable.ShouldBe( 40 );

            return dataSet;
        }

        protected override async Task<DataSet> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            Int32 rowsInFirstTable = await adapter.Fill1Async( dataSet );
            rowsInFirstTable.ShouldBe( 40 );

            return dataSet;
        }

        protected override void AssertResult(DataSet dbSynchronous, DataSet dbProxied, DataSet dbProxiedAsync, DataSet dbBatchingProxiedAsync)
        {
            DataTableMethods.DataSetEquals( dbSynchronous, dbProxied             , out String diffs1 ).ShouldBeTrue( customMessage: diffs1 );
            DataTableMethods.DataSetEquals( dbSynchronous, dbProxiedAsync        , out String diffs2 ).ShouldBeTrue( customMessage: diffs2 );
            DataTableMethods.DataSetEquals( dbSynchronous, dbBatchingProxiedAsync, out String diffs3 ).ShouldBeTrue( customMessage: diffs3 );
        }
    }
}
