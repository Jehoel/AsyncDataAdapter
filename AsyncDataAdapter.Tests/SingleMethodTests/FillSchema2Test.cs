using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests
{
    using FS2Pair = ValueTuple<DataTable,DataTable>;

    public abstract class FillSchema2Test : SingleMethodTest<FS2Pair>
    {
        protected FillSchema2Test( SchemaType schemaType )
        {
            this.SchemaType = schemaType;
        }

        protected SchemaType SchemaType { get; }

        protected override FS2Pair RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            DataTable schemaTables = adapter.FillSchema2( dataTable, schemaType: this.SchemaType );

            return ( dataTable, schemaTables );
        }

        protected override FS2Pair RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            DataTable schemaTables = adapter.FillSchema2( dataTable, schemaType: this.SchemaType );

            return ( dataTable, schemaTables );
        }

        protected override async Task<FS2Pair> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            DataTable schemaTables = await adapter.FillSchema2Async( dataTable, schemaType: this.SchemaType );

            return ( dataTable, schemaTables );
        }

        protected override async Task<FS2Pair> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataTable dataTable = new DataTable();

            DataTable schemaTables = await adapter.FillSchema2Async( dataTable, schemaType: this.SchemaType );

            return ( dataTable, schemaTables );
        }

        protected override void AssertResult( FS2Pair dbSynchronous, FS2Pair dbProxied, FS2Pair dbProxiedAsync, FS2Pair dbBatchingProxiedAsync )
        {
            throw new NotImplementedException();
        }
    }

    public class FillSchema2MappedTest : FillSchema2Test
    {
        public FillSchema2MappedTest()
            : base( SchemaType.Mapped )
        {
        }
    }

    public class FillSchema2SourceTest : FillSchema2Test
    {
        public FillSchema2SourceTest()
            : base( SchemaType.Source )
        {
        }
    }
}
