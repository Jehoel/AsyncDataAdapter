using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    public static partial class AsyncDataReaderMethods
    {
        public static async Task<DataTable[]> FillSchemaAsync( IAdaSchemaMappingAdapter adapter, Boolean returnProviderSpecificTypes, DataSet dataSet, SchemaType schemaType, string srcTable, DbDataReader dataReader, CancellationToken cancellationToken )
        {
            if (null == dataSet) throw new ArgumentNullException(nameof(dataSet));
            if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType)) throw ADP.InvalidSchemaType(schemaType);
            if (string.IsNullOrEmpty(srcTable)) throw ADP.FillSchemaRequiresSourceTableName("srcTable");
            if ((null == dataReader) || dataReader.IsClosed) throw ADP.FillRequires("dataReader");

            // user must Close/Dispose of the dataReader
            return await FillSchemaFromReaderAsync( adapter, returnProviderSpecificTypes, dataSet, singleDataTable: null, schemaType, srcTable, dataReader, cancellationToken ).ConfigureAwait(false);
        }

        public static async Task<DataTable> FillSchemaAsync( IAdaSchemaMappingAdapter adapter, Boolean returnProviderSpecificTypes, DataTable dataTable, SchemaType schemaType, DbDataReader dataReader, CancellationToken cancellationToken )
        {
            if (null == dataTable) throw new ArgumentNullException(nameof(dataTable));
            if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType)) throw ADP.InvalidSchemaType(schemaType);
            if ((null == dataReader) || dataReader.IsClosed)throw ADP.FillRequires("dataReader");

            // user must Close/Dispose of the dataReader
            // user will have to call NextResult to access remaining results
            DataTable[] singleTable = await FillSchemaFromReaderAsync( adapter, returnProviderSpecificTypes, dataset: null, singleDataTable: dataTable, schemaType, srcTable: null, dataReader, cancellationToken ).ConfigureAwait(false);
            if( singleTable is DataTable[] arr && arr.Length == 1 ) return singleTable[0];
            return null;
        }

        public static async Task<DataTable[]> FillSchemaFromReaderAsync( IAdaSchemaMappingAdapter adapter, Boolean returnProviderSpecificTypes, DataSet dataset, DataTable singleDataTable, SchemaType schemaType, string srcTable, DbDataReader dataReader, CancellationToken cancellationToken )
        {
            DataTable[] dataTables = null;
            int schemaCount = 0;
            do
            {
                AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dataReader, useProviderSpecificDataReader: returnProviderSpecificTypes );
                if (0 >= readerHandler.FieldCount)
                {
                    continue;
                }

                string sourceTableName = null;
                if (null != dataset)
                {
                    sourceTableName = GetSourceTableName(srcTable, schemaCount);
                    schemaCount++; // don't increment if no SchemaTable ( a non-row returning result )
                }

                AdaSchemaMapping mapping = new AdaSchemaMapping( adapter: adapter, dataset, singleDataTable, dataReader: readerHandler, keyInfo: true, schemaType, sourceTableName, gettingData: false, parentChapterColumn: null, parentChapterValue: null );

                if (singleDataTable != null)
                {
                    // do not read remaining results in single DataTable case
                    return new DataTable[] { mapping.DataTable };
                }
                else if (null != mapping.DataTable)
                {
                    if (null == dataTables)
                    {
                        dataTables = new DataTable[1] { mapping.DataTable };
                    }
                    else
                    {
                        dataTables = AddDataTableToArray( dataTables, mapping.DataTable );
                    }
                }
            }
            while ( await dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false) ); // FillSchema does not capture errors for FillError event

            if( dataTables is null && singleDataTable is null )
            {
                return Array.Empty<DataTable>();
            }
            else
            {
                return dataTables;
            }
        }

        // used by FillSchema which returns an array of datatables added to the dataset
        public static DataTable[] AddDataTableToArray(DataTable[] tables, DataTable newTable)
        {
            for (int i = 0; i < tables.Length; ++i)
            {
                // search for duplicates:
                if (Object.ReferenceEquals( tables[i], newTable ))
                {
                    return tables; // duplicate found
                }
            }
            DataTable[] newTables = new DataTable[tables.Length + 1]; // add unique data table
            for (int i = 0; i < tables.Length; ++i)
            {
                newTables[i] = tables[i];
            }
            newTables[tables.Length] = newTable;
            return newTables;
        }
    }
}
