using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests.Big3
{
    using FS3Pair = ValueTuple<DataSet,DataTable[]>;

    public abstract class FillSchema3Test : SingleMethodTest<FS3Pair>
    {
        protected FillSchema3Test( SchemaType schemaType )
        {
            this.SchemaType = schemaType;
        }

        protected SchemaType SchemaType { get; }

        protected override FS3Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            DataTable[] schemaTables = adapter.FillSchema3( dataSet, schemaType: this.SchemaType, srcTable: "Foobar" );

            return ( dataSet, schemaTables );
        }

        protected override FS3Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            DataTable[] schemaTables = adapter.FillSchema3( dataSet, schemaType: this.SchemaType, srcTable: "Foobar" );

            return ( dataSet, schemaTables );
        }

        protected override async Task<FS3Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            DataTable[] schemaTables = await adapter.FillSchema3Async( dataSet, schemaType: this.SchemaType, srcTable: "Foobar" );

            return ( dataSet, schemaTables );
        }

        protected override async Task<FS3Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataSet dataSet = new DataSet();

            DataTable[] schemaTables = await adapter.FillSchema3Async( dataSet, schemaType: this.SchemaType, srcTable: "Foobar" );

            return ( dataSet, schemaTables );
        }

        protected override void AssertResult( FS3Pair dbSynchronous, FS3Pair dbProxied, FS3Pair dbProxiedAsync, FS3Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }

    public class FillSchema3MappedTest : FillSchema3Test
    {
        public FillSchema3MappedTest()
            : base( SchemaType.Mapped )
        {
        }
    }

    public class FillSchema3SourceTest : FillSchema3Test
    {
        public FillSchema3SourceTest()
            : base( SchemaType.Source )
        {
        }
    }
}
