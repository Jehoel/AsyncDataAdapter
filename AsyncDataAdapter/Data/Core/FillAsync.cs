using System;
using System.Collections.Generic;
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
        public static async Task<Int32> FillAsync( Action<Exception, DataTable, Object[]> onFillError, IAdaSchemaMappingAdapter adapter, Boolean returnProviderSpecificTypes, DataTable[] dataTables, DbDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            if (dataTables is null) throw new ArgumentNullException(paramName: nameof(dataTables));
            if (0 == dataTables.Length) throw new ArgumentException(string.Format("Argument is empty: {0}", nameof(dataTables)), paramName: nameof(dataTables));

            if (null == dataTables[0]) throw ADP.FillRequires("dataTable");
            if (null == dataReader) throw ADP.FillRequires("dataReader");
            if ((1 < dataTables.Length) && ((0 != startRecord) || (0 != maxRecords))) throw new NotSupportedException(); // FillChildren is not supported with FillPage

            //

            int result = 0;
            bool enforceContraints = false;
            DataSet commonDataSet = dataTables[0].DataSet;
            try
            {
                if (null != commonDataSet)
                {
                    enforceContraints = commonDataSet.EnforceConstraints;
                    commonDataSet.EnforceConstraints = false;
                }
                for (int i = 0; i < dataTables.Length; ++i)
                {
                    Debug.Assert(null != dataTables[i], "null DataTable Fill");

                    if (dataReader.IsClosed)
                    {
                        break;
                    }

                    AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dataReader, useProviderSpecificDataReader: returnProviderSpecificTypes );
                    if (readerHandler.FieldCount <= 0)
                    {
                        if (i == 0)
                        {
                            bool lastFillNextResult;
                            do
                            {
                                lastFillNextResult = await FillNextResultAsync( onFillError, readerHandler, cancellationToken ).ConfigureAwait(false);
                            }
                            while (lastFillNextResult && readerHandler.FieldCount <= 0);
                                
                            if (!lastFillNextResult)
                            {
                                break;
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }
                       
                    if ((0 < i) && !await FillNextResultAsync( onFillError, readerHandler, cancellationToken ).ConfigureAwait(false))
                    {
                        break;
                    }
                    // user must Close/Dispose of the dataReader
                    // user will have to call NextResult to access remaining results
                    int count = await FillFromReaderAsync( onFillError, adapter, null, dataTables[i], null, readerHandler, startRecord, maxRecords, null, null, cancellationToken ).ConfigureAwait(false);
                    if (0 == i)
                    {
                        result = count;
                    }
                }
            }
            catch (ConstraintException)
            {
                enforceContraints = false;
                throw;
            }
            finally
            {
                if (enforceContraints)
                {
                    commonDataSet.EnforceConstraints = true;
                }
            }

            return result;
        }

        public static async Task<int> FillFromReaderAsync( Action<Exception, DataTable, Object[]> onFillError, IAdaSchemaMappingAdapter adapter, DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken )
        {
Stopwatch sw = Stopwatch.StartNew();
List<(TimeSpan,String)> list = new List<(TimeSpan, string)>();

            int rowsAddedToDataSet = 0;
            int schemaCount = 0;
            do
            {
list.Add( ( sw.Elapsed, "Loop body entered completed" ) );
                if (0 >= dataReader.FieldCount)
                {
                    continue; // loop to next result
                }

                AdaSchemaMapping mapping = FillMapping( onFillError, adapter, dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
                schemaCount++; // don't increment if no SchemaTable ( a non-row returning result )

list.Add( ( sw.Elapsed, "FillMapping completed" ) );

                if (null == mapping) continue; // loop to next result
                if (null == mapping.DataValues) continue; // loop to next result
                if (null == mapping.DataTable) continue; // loop to next result

                mapping.DataTable.BeginLoadData();

                try
                {
                    // startRecord and maxRecords only apply to the first resultset
                    if ((1 == schemaCount) && ((0 < startRecord) || (0 < maxRecords)))
                    {
                        rowsAddedToDataSet = await FillLoadDataRowChunkAsync( onFillError, mapping, startRecord, maxRecords, cancellationToken ).ConfigureAwait(false);
list.Add( ( sw.Elapsed, "FillLoadDataRowChunkAsync completed" ) );
                    }
                    else
                    {
                        int count = await FillLoadDataRowAsync( onFillError, mapping, cancellationToken ).ConfigureAwait(false);
list.Add( ( sw.Elapsed, "FillLoadDataRowAsync completed" ) );

                        if (1 == schemaCount)
                        {
                            // only return LoadDataRow count for first resultset
                            // not secondary or chaptered results
                            rowsAddedToDataSet = count;
                        }
                    }
                }
                finally
                {
                    mapping.DataTable.EndLoadData();
                }
                if (null != datatable)
                {
                    break; // do not read remaining results in single DataTable case
                }

list.Add( ( sw.Elapsed, "Loop body completed" ) );
            }
            while( await FillNextResultAsync( onFillError, dataReader, cancellationToken ).ConfigureAwait(false) );

            return rowsAddedToDataSet;
        }

        public static async Task<bool> FillNextResultAsync( Action<Exception, DataTable, Object[]> onFillError, AdaDataReaderContainer dataReader, CancellationToken cancellationToken )
        {
            bool result = true;
            if ( onFillError != null )
            {
                try
                {
                    // only try-catch if a FillErrorEventHandler is registered so that
                    // in the default case we get the full callstack from users
                    result = await dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (!ADP.IsCatchableExceptionType(ex))
                    {
                        throw;
                    }

                    onFillError.Invoke( ex, null, null );
                }
            }
            else
            {
                result = await dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false);
            }

            return result;
        }

        
    }
}
