using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests.Big3
{
    using FS1Pair = ValueTuple<DataSet,DataTable[]>;

    public abstract class FillSchema1Test : SingleMethodTest<FS1Pair>
    {
        protected FillSchema1Test( SchemaType schemaType )
        {
            this.SchemaType = schemaType;
        }

        protected SchemaType SchemaType { get; }

        protected override FS1Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            DataTable[] schemaTables = adapter.FillSchema1( dataSet, schemaType: this.SchemaType );

            return ( dataSet, schemaTables );
        }

        protected override FS1Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            DataTable[] schemaTables = adapter.FillSchema1( dataSet, schemaType: this.SchemaType );

            return ( dataSet, schemaTables );
        }

        protected override async Task<FS1Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            DataTable[] schemaTables = await adapter.FillSchema1Async( dataSet, schemaType: this.SchemaType );

            return ( dataSet, schemaTables );
        }

        protected override async Task<FS1Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            DataTable[] schemaTables = await adapter.FillSchema1Async( dataSet, schemaType: this.SchemaType );

            return ( dataSet, schemaTables );
        }

        protected override void AssertResult( FS1Pair dbSynchronous, FS1Pair dbProxied, FS1Pair dbProxiedAsync, FS1Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }

    public class FillSchema1MappedTest : FillSchema1Test
    {
        public FillSchema1MappedTest()
            : base( SchemaType.Mapped )
        {
        }
    }

    public class FillSchema1SourceTest : FillSchema1Test
    {
        public FillSchema1SourceTest()
            : base( SchemaType.Source )
        {
        }
    }
}
