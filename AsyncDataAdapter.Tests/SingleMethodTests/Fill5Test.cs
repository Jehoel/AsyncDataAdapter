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
        // When using Fill5 with DataTable[] arrays of length other-than 1, the startRecord and maxRecord values must both be zero.
        // See `throw ADP.OnlyOneTableForStartRecordOrMaxRecords();` in the reference-source.

        protected override DataTable[] RunDbDataAdapterSynchronous(List<TestTable> randomDataSource, FakeDbDataAdapter adapter)
        {
            DataTable[] tables = new DataTable[5]
            {
                new DataTable( tableName: "Foo", tableNamespace: "NS1" ),
                new DataTable( tableName: "Bar", tableNamespace: "NS1" ),
                new DataTable( tableName: "Baz", tableNamespace: "NS1" ),
                new DataTable( tableName: "Qux", tableNamespace: "NS1" ),
                new DataTable( tableName: "Tux", tableNamespace: "NS1" )
            };

            Int32 rowsInFirstTable = adapter.Fill5( startRecord: 0, maxRecords: 0, dataTables: tables );
            rowsInFirstTable.ShouldBe( 40 );

            return tables;
        }

        protected override DataTable[] RunProxiedDbDataAdapter(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable[] tables = new DataTable[5]
            {
                new DataTable( tableName: "Foo", tableNamespace: "NS1" ),
                new DataTable( tableName: "Bar", tableNamespace: "NS1" ),
                new DataTable( tableName: "Baz", tableNamespace: "NS1" ),
                new DataTable( tableName: "Qux", tableNamespace: "NS1" ),
                new DataTable( tableName: "Tux", tableNamespace: "NS1" )
            };

            Int32 rowsInFirstTable = adapter.Fill5( startRecord: 0, maxRecords: 0, dataTables: tables );
            rowsInFirstTable.ShouldBe( 40 );

            return tables;
        }

        protected override async Task<DataTable[]> RunProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter)
        {
            DataTable[] tables = new DataTable[5]
            {
                new DataTable( tableName: "Foo", tableNamespace: "NS1" ),
                new DataTable( tableName: "Bar", tableNamespace: "NS1" ),
                new DataTable( tableName: "Baz", tableNamespace: "NS1" ),
                new DataTable( tableName: "Qux", tableNamespace: "NS1" ),
                new DataTable( tableName: "Tux", tableNamespace: "NS1" )
            };

            Int32 rowsInFirstTable = await adapter.Fill5Async( startRecord: 0, maxRecords: 0, dataTables: tables );
            rowsInFirstTable.ShouldBe( 40 );

            return tables;
        }

        protected override async Task<DataTable[]> RunBatchingProxiedDbDataAdapterAsync(List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter)
        {
            DataTable[] tables = new DataTable[5]
            {
                new DataTable( tableName: "Foo", tableNamespace: "NS1" ),
                new DataTable( tableName: "Bar", tableNamespace: "NS1" ),
                new DataTable( tableName: "Baz", tableNamespace: "NS1" ),
                new DataTable( tableName: "Qux", tableNamespace: "NS1" ),
                new DataTable( tableName: "Tux", tableNamespace: "NS1" )
            };

            Int32 rowsInFirstTable = await adapter.Fill5Async( startRecord: 0, maxRecords: 0, dataTables: tables );
            rowsInFirstTable.ShouldBe( 40 );

            return tables;
        }

        protected override void AssertResult(DataTable[] dbSynchronous, DataTable[] dbProxied, DataTable[] dbProxiedAsync, DataTable[] dbBatchingProxiedAsync)
        {
            DataTableMethods.DataTablesEquals( dbSynchronous, dbProxied             , out String diffs1 ).ShouldBeTrue( customMessage: diffs1 );
            DataTableMethods.DataTablesEquals( dbSynchronous, dbProxiedAsync        , out String diffs2 ).ShouldBeTrue( customMessage: diffs2 );
            DataTableMethods.DataTablesEquals( dbSynchronous, dbBatchingProxiedAsync, out String diffs3 ).ShouldBeTrue( customMessage: diffs3 );
        }
    }
}
