using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

using Shouldly;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests.Big3
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
            DataTableMethods.DataTableEquals( dbSynchronous.Item1, dbProxied             .Item1, "1", out String diffs11 ).ShouldBeTrue( customMessage: diffs11 );
            DataTableMethods.DataTableEquals( dbSynchronous.Item1, dbProxiedAsync        .Item1, "2", out String diffs12 ).ShouldBeTrue( customMessage: diffs12 );
            DataTableMethods.DataTableEquals( dbSynchronous.Item1, dbBatchingProxiedAsync.Item1, "3", out String diffs13 ).ShouldBeTrue( customMessage: diffs13 );

            DataTableMethods.DataTableEquals( dbSynchronous.Item2, dbProxied             .Item2, "1", out String diffs21 ).ShouldBeTrue( customMessage: diffs21 );
            DataTableMethods.DataTableEquals( dbSynchronous.Item2, dbProxiedAsync        .Item2, "2", out String diffs22 ).ShouldBeTrue( customMessage: diffs22 );
            DataTableMethods.DataTableEquals( dbSynchronous.Item2, dbBatchingProxiedAsync.Item2, "3", out String diffs23 ).ShouldBeTrue( customMessage: diffs23 );
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
