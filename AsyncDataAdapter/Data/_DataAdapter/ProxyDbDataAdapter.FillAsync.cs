using System;
using System.Data;
using System.Data.Common;
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
            TDbConnection   connection    = GetConnection( command );
		    ConnectionState originalState = ConnectionState.Open;

		    if( MissingSchemaAction.AddWithKey == this.MissingSchemaAction )
		    {
			    behavior |= CommandBehavior.KeyInfo;
		    }

		    try
		    {
			    originalState = await QuietOpenAsync( connection, cancellationToken ).ConfigureAwait(false);

			    behavior |= CommandBehavior.SequentialAccess;
			        
                using( TDbDataReader dataReader = await ExecuteReaderAsync( command, behavior, cancellationToken ).ConfigureAwait(false) )
                {
                    if (datatables != null)
				    {
					    return await this.FillAsync( datatables, dataReader, startRecord, maxRecords, cancellationToken ).ConfigureAwait(false);
				    }
                    else
                    {
                        return await this.FillAsync( dataset, srcTable, dataReader, startRecord, maxRecords, cancellationToken ).ConfigureAwait(false);
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

            return this.FillFromReaderAsync( dataSet, null, srcTable, readerHandler, startRecord, maxRecords, null, null, cancellationToken );
        }

        //

        internal Task<Int32> FillFromReaderAsync( DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken )
        {
	        return AsyncDataReaderMethods.FillFromReaderAsync( onFillError: null, adapter: this, dataset, datatable, srcTable, dataReader, startRecord, maxRecords: maxRecords, parentChapterColumn, parentChapterValue, cancellationToken );
        }

        private AdaSchemaMapping FillMapping(DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {
            Action<Exception, DataTable, Object[]> onFillError = this.GetCurrentFillErrorHandler();

            return AsyncDataReaderMethods.FillMapping( onFillError, this, dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
        }
    }
}
