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
        public static async Task<int> FillLoadDataRowChunkAsync( Action<Exception, DataTable, Object[]> onFillError, AdaSchemaMapping mapping, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            AdaDataReaderContainer dataReader = mapping.DataReader;

            while (0 < startRecord)
            {
                if (!await dataReader.ReadAsync( cancellationToken ).ConfigureAwait(false))
                {
                    // there are no more rows on first resultset
                    return 0;
                }
                --startRecord;
            }

            int rowsAddedToDataSet = 0;
            if (0 < maxRecords)
            {
                while ((rowsAddedToDataSet < maxRecords) && await dataReader.ReadAsync( cancellationToken ).ConfigureAwait(false))
                {
                    if (onFillError != null)
                    {
                        try
                        {
                            await mapping.LoadDataRowWithClearAsync( cancellationToken ).ConfigureAwait(false);
                            rowsAddedToDataSet++;
                        }
                        catch (Exception e)
                        {
                            // 
                            if (!ADP.IsCatchableExceptionType(e))
                            {
                                throw;
                            }

                            onFillError(e, mapping.DataTable, mapping.DataValues);
                        }
                    }
                    else
                    {
                        await mapping.LoadDataRowAsync( cancellationToken ).ConfigureAwait(false);
                        rowsAddedToDataSet++;
                    }
                }
                // skip remaining rows of the first resultset
            }
            else
            {
                rowsAddedToDataSet = await FillLoadDataRowAsync( onFillError, mapping, cancellationToken ).ConfigureAwait(false);
            }
            return rowsAddedToDataSet;
        }

        public static async Task<int> FillLoadDataRowAsync( Action<Exception, DataTable, Object[]> onFillError, AdaSchemaMapping mapping, CancellationToken cancellationToken )
        {
Stopwatch sw = Stopwatch.StartNew();
List<(TimeSpan,String)> list = new List<(TimeSpan, string)>();

            int rowsAddedToDataSet = 0;
            AdaDataReaderContainer dataReader = mapping.DataReader;
            if ( onFillError != null )
            {
                while (await dataReader.ReadAsync( cancellationToken ).ConfigureAwait(false))
                {
list.Add( ( sw.Elapsed, "ReadAsync 1 completed" ) );

                    // read remaining rows of first and subsequent resultsets
                    try
                    {
                        // only try-catch if a FillErrorEventHandler is registered so that
                        // in the default case we get the full callstack from users
                        await mapping.LoadDataRowWithClearAsync( cancellationToken ).ConfigureAwait(false);
list.Add( ( sw.Elapsed, "LoadDataRowWithClearAsync completed" ) );
                        rowsAddedToDataSet++;
                    }
                    catch (Exception e)
                    {
                        // 
                        if (!ADP.IsCatchableExceptionType(e))
                        {
                            throw;
                        }

                        onFillError(e, mapping.DataTable, mapping.DataValues);
                    }
                }
            }
            else
            {
                while (await dataReader.ReadAsync( cancellationToken ).ConfigureAwait(false))
                {
list.Add( ( sw.Elapsed, "ReadAsync 2 completed" ) );
                    // read remaining rows of first and subsequent resultset
                    await mapping.LoadDataRowAsync( cancellationToken ).ConfigureAwait(false);
list.Add( ( sw.Elapsed, "LoadDataRowAsync 2 completed" ) );
                    rowsAddedToDataSet++;
                }
            }
            return rowsAddedToDataSet;
        }
    }
}
