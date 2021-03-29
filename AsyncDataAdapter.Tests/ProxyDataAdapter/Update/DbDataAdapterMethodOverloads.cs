using System;
using System.Data;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter.Tests
{
    /// <summary>This class introduces explicitly named extension methods to avoid ambiguity problems when writing code for specific overloads.</summary>
    public static class DbDataAdapterMethodOverloads
    {
        #region Fill

        public static Int32 Fill1( this IFullDbDataAdapter adapter, DataSet dataSet)
        {
            return adapter.Fill( dataSet: dataSet );
        }

        public static Task<Int32> Fill1Async( this IFullAsyncDbDataAdapter adapter, DataSet dataSet )
        {
            return adapter.FillAsync( dataSet: dataSet, cancellationToken: default );
        }

        //

        public static Int32 Fill2( this IFullDbDataAdapter adapter, DataSet dataSet, string srcTable )
        {
            return adapter.Fill( dataSet: dataSet, srcTable: srcTable );
        }

        public static Task<Int32> Fill2Async( this IFullAsyncDbDataAdapter adapter, DataSet dataSet, string srcTable )
        {
            return adapter.FillAsync( dataSet: dataSet, srcTable: srcTable, cancellationToken: default );
        }

        //

        public static Int32 Fill3( this IFullDbDataAdapter adapter, DataSet dataSet, int startRecord, int maxRecords, string srcTable )
        {
            return adapter.Fill( dataSet: dataSet, startRecord: startRecord, maxRecords: maxRecords, srcTable: srcTable );
        }

        public static Task<Int32> Fill3Async( this IFullAsyncDbDataAdapter adapter, DataSet dataSet, int startRecord, int maxRecords, string srcTable )
        {
            return adapter.FillAsync( dataSet: dataSet, startRecord: startRecord, maxRecords: maxRecords, srcTable: srcTable, cancellationToken: default );
        }

        //

        public static Int32 Fill4( this IFullDbDataAdapter adapter, DataTable dataTable )
        {
            return adapter.Fill( dataTable: dataTable );
        }

        public static Task<Int32> Fill4Async( this IFullAsyncDbDataAdapter adapter, DataTable dataTable )
        {
            return adapter.FillAsync( dataTable: dataTable, cancellationToken: default );
        }

        //

        public static Int32 Fill5( this IFullDbDataAdapter adapter, int startRecord, int maxRecords, DataTable[] dataTables )
        {
            return adapter.Fill( startRecord: startRecord, maxRecords: maxRecords, dataTables: dataTables );
        }

        public static Task<Int32> Fill5Async( this IFullAsyncDbDataAdapter adapter, int startRecord, int maxRecords, DataTable[] dataTables )
        {
            return adapter.FillAsync( startRecord: startRecord, maxRecords: maxRecords, dataTables: dataTables, cancellationToken: default );
        }

        #endregion

        #region FillSchema

        public static DataTable[] FillSchema1( this IFullDbDataAdapter adapter, DataSet dataSet, SchemaType schemaType)
        {
            return adapter.FillSchema( dataSet: dataSet, schemaType: schemaType );
        }

        public static Task<DataTable[]> FillSchema1Async( this IFullAsyncDbDataAdapter adapter, DataSet dataSet, SchemaType schemaType )
        {
            return adapter.FillSchemaAsync( dataSet: dataSet, schemaType: schemaType, cancellationToken: default );
        }

        //

        public static DataTable FillSchema2( this IFullDbDataAdapter adapter, DataTable dataTable, SchemaType schemaType )
        {
            return adapter.FillSchema( dataTable: dataTable, schemaType: schemaType );
        }

        public static Task<DataTable> FillSchema2Async( this IFullAsyncDbDataAdapter adapter, DataTable dataTable, SchemaType schemaType )
        {
            return adapter.FillSchemaAsync( dataTable: dataTable, schemaType: schemaType, cancellationToken: default );
        }

        //

        public static DataTable[] FillSchema3( this IFullDbDataAdapter adapter, DataSet dataSet, SchemaType schemaType, string srcTable)
        {
            return adapter.FillSchema( dataSet: dataSet, schemaType: schemaType, srcTable: srcTable );
        }

        public static Task<DataTable[]> FillSchema3Async( this IFullAsyncDbDataAdapter adapter, DataSet dataSet, SchemaType schemaType, string srcTable )
        {
            return adapter.FillSchemaAsync( dataSet: dataSet, schemaType: schemaType, srcTable: srcTable, cancellationToken: default );
        }

        #endregion

        #region Update

        public static Int32 Update1( this IFullDbDataAdapter adapter, DataSet dataSet )
        {
            return adapter.Update( dataSet: dataSet );
        }

        public static Task<Int32> Update1Async( this IFullAsyncDbDataAdapter adapter, DataSet dataSet )
        {
            return adapter.UpdateAsync( dataSet: dataSet, cancellationToken: default );
        }

        //

        public static Int32 Update2( this IFullDbDataAdapter adapter, DataRow[] dataRows )
        {
            return adapter.Update( dataRows: dataRows );
        }

        public static Task<Int32> Update2Async( this IFullAsyncDbDataAdapter adapter, DataRow[] dataRows )
        {
            return adapter.UpdateAsync( dataRows: dataRows, cancellationToken: default );
        }

        //

        public static Int32 Update3( this IFullDbDataAdapter adapter, DataTable dataTable)
        {
            return adapter.Update( dataTable: dataTable );
        }

        public static Task<Int32> Update3Async( this IFullAsyncDbDataAdapter adapter, DataTable dataTable )
        {
            return adapter.UpdateAsync( dataTable: dataTable, cancellationToken: default );
        }

        //

        public static Int32 Update4( this IFullDbDataAdapter adapter, DataSet dataSet, string srcTable )
        {
            return adapter.Update( dataSet: dataSet, srcTable: srcTable );
        }

        public static Task<Int32> Update4Async( this IFullAsyncDbDataAdapter adapter, DataSet dataSet, string srcTable )
        {
            return adapter.UpdateAsync( dataSet: dataSet, srcTable: srcTable, cancellationToken: default );
        }

        #endregion

        
    }
}
