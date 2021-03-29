using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> : IUpdatingAsyncDbDataAdapter, IAdaSchemaMappingAdapter
    {
        #region IAdaSchemaMappingAdapter

        DataTableMapping IAdaSchemaMappingAdapter.GetTableMappingBySchemaAction( string sourceTableName, string dataSetTableName, MissingMappingAction mappingAction )
        {
            return DataTableMappingCollection.GetTableMappingBySchemaAction( this.TableMappings, sourceTableName, dataSetTableName, mappingAction );
        }

        int IAdaSchemaMappingAdapter.IndexOfDataSetTable(string dataSetTable)
        {
            return this.TableMappings?.IndexOfDataSetTable( dataSetTable ) ?? -1;
        }

        Task<int> IAdaSchemaMappingAdapter.FillFromReaderAsync( DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken )
        {
            Action<Exception, DataTable, Object[]> onFillError = this.GetCurrentFillErrorHandler();

            return AsyncDataReaderMethods.FillFromReaderAsync( onFillError, this, dataset, datatable, srcTable, dataReader, startRecord, maxRecords, parentChapterColumn, parentChapterValue, cancellationToken );
        }

        #endregion

        protected virtual Task<Int32> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, TDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
		    if (dataSet == null) throw ADP.FillRequires("dataSet");
		    if (startRecord < 0) throw ADP.InvalidStartRecord("startRecord", startRecord);
		    if (maxRecords < 0) throw ADP.InvalidMaxRecords("maxRecords", maxRecords);
		    if (String.IsNullOrEmpty(srcTable)) throw ADP.FillRequiresSourceTableName("srcTable");
		    if (command == null) throw ADP.MissingSelectCommand("Fill");

		    return this.FillInternalAsync( dataSet, datatables: null, startRecord: startRecord, maxRecords: maxRecords, srcTable, command, behavior, cancellationToken );
        }

        protected virtual Task<Int32> FillAsync( DataTable[] dataTables, int startRecord, int maxRecords, TDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            if (dataTables == null || dataTables.Length == 0 || dataTables[0] == null) throw ADP.FillRequires("dataTable");
		    if (startRecord < 0) throw ADP.InvalidStartRecord("startRecord", startRecord);
		    if (maxRecords < 0) throw ADP.InvalidMaxRecords("maxRecords", maxRecords);
		    if (1 < dataTables.Length && (startRecord != 0 || maxRecords != 0)) throw ADP.OnlyOneTableForStartRecordOrMaxRecords();
		    if (command == null) throw ADP.MissingSelectCommand("Fill");

            //

		    if (1 == dataTables.Length)
		    {
			    behavior |= CommandBehavior.SingleResult;
		    }

		    return this.FillInternalAsync( null, dataTables, startRecord, maxRecords, null, command, behavior, cancellationToken );
        }

        private async Task<Int32> FillInternalAsync( DataSet dataset, DataTable[] datatables, int startRecord, int maxRecords, string srcTable, TDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
Stopwatch sw = Stopwatch.StartNew();
List<(TimeSpan,String)> list = new List<(TimeSpan, string)>();

            TDbConnection   connection    = GetConnection( command );
		    ConnectionState originalState = ConnectionState.Open;

		    if( MissingSchemaAction.AddWithKey == this.MissingSchemaAction )
		    {
			    behavior |= CommandBehavior.KeyInfo;
		    }

		    try
		    {
			    originalState = await QuietOpenAsync( connection, cancellationToken ).ConfigureAwait(false);

list.Add( ( sw.Elapsed, "QuietOpenAsync completed" ) );

			    behavior |= CommandBehavior.SequentialAccess;
			        
                using( TDbDataReader dataReader = await ExecuteReaderAsync( command, behavior, cancellationToken ).ConfigureAwait(false) )
                {
list.Add( ( sw.Elapsed, "ExecuteReaderAsync completed" ) );

                    if (datatables != null)
				    {
					    var x = await this.FillAsync( datatables, dataReader, startRecord, maxRecords, cancellationToken ).ConfigureAwait(false);
list.Add( ( sw.Elapsed, "FillAsync completed" ) );
                        return x;
				    }
                    else
                    {
                        var y = await this.FillAsync( dataset, srcTable, dataReader, startRecord, maxRecords, cancellationToken ).ConfigureAwait(false);
list.Add( ( sw.Elapsed, "FillAsync completed" ) );
                        return y;
                    }
                }
		    }
		    finally
		    {
			    QuietClose( connection, originalState );
		    }
        }

        protected virtual Task<Int32> FillAsync( DataTable[] dataTables, DbDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
		    return AsyncDataReaderMethods.FillAsync( onFillError: null, adapter: this, this.ReturnProviderSpecificTypes, dataTables, dataReader, startRecord, maxRecords: maxRecords, cancellationToken );
        }

        protected virtual Task<Int32> FillAsync( DataSet dataSet, string srcTable, DbDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            if (null == dataSet) throw ADP.FillRequires("dataSet");
            if (string.IsNullOrEmpty(srcTable)) throw ADP.FillRequiresSourceTableName("srcTable");
            if (null == dataReader) throw ADP.FillRequires("dataReader");
            if (startRecord < 0) throw ADP.InvalidStartRecord("startRecord", startRecord);
            if (maxRecords < 0) throw ADP.InvalidMaxRecords("maxRecords", maxRecords);

            //

            if (dataReader.IsClosed)
            {
                return Task.FromResult( 0 );
            }

            // user must Close/Dispose of the dataReader
            AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dataReader, useProviderSpecificDataReader: this.ReturnProviderSpecificTypes );

            return this.FillFromReaderAsync(
                dataset            : dataSet,
                datatable          : null,
                srcTable           : srcTable,
                dataReader         : readerHandler,
                startRecord        : startRecord,
                maxRecords         : maxRecords,
                parentChapterColumn: null,
                parentChapterValue : null,
                cancellationToken  : cancellationToken
            );
        }

        //

        internal Task<Int32> FillFromReaderAsync( DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken )
        {
	        return AsyncDataReaderMethods.FillFromReaderAsync(
                onFillError        : null,
                adapter            : this,
                dataset            : dataset,
                datatable          : datatable,
                srcTable           : srcTable,
                dataReader         : dataReader,
                startRecord        : startRecord,
                maxRecords         : maxRecords,
                parentChapterColumn: parentChapterColumn,
                parentChapterValue : parentChapterValue,
                cancellationToken  : cancellationToken
            );
        }

        private AdaSchemaMapping FillMapping(DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {
            Action<Exception, DataTable, Object[]> onFillError = this.GetCurrentFillErrorHandler();

            return AsyncDataReaderMethods.FillMapping( onFillError, this, dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
        }

        #region Non-virtual entrypoints

        public Task<int> FillAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken = default)
        {
            return this.FillAsync( dataSet: dataSet, startRecord: 0, maxRecords: 0, srcTable: srcTable, command: this.SelectCommand, behavior: this.FillCommandBehavior, cancellationToken: cancellationToken );
        }

        public Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, CancellationToken cancellationToken = default)
        {
            return this.FillAsync( dataSet: dataSet, startRecord: startRecord, maxRecords: maxRecords, srcTable: srcTable, command: this.SelectCommand, behavior: this.FillCommandBehavior, cancellationToken: cancellationToken );
        }

        public Task<int> FillAsync(DataTable dataTable, CancellationToken cancellationToken = default)
        {
            DataTable[] dataTables = new DataTable[1] { dataTable };

            return this.FillAsync( dataTables, startRecord: 0, maxRecords: 0, command: this.SelectCommand, behavior: this.FillCommandBehavior, cancellationToken: cancellationToken );
        }

        public Task<int> FillAsync(int startRecord, int maxRecords, DataTable[] dataTables, CancellationToken cancellationToken = default)
        {
            return this.FillAsync( dataTables, startRecord: startRecord, maxRecords: maxRecords, command: this.SelectCommand, behavior: this.FillCommandBehavior, cancellationToken: cancellationToken );
        }

        #endregion
    }
}
