using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter.Internal
{
    public class SchemaMapping2
    {
        private readonly Object instance;

        public SchemaMapping2( Object instance )
        {
            this.instance = instance ?? throw new ArgumentNullException(nameof(instance)); 
        }
    }

    public class DataReaderContainer2
    {
        private readonly Object instance;

        public DataReaderContainer2( Object instance )
        {
            this.instance = instance ?? throw new ArgumentNullException(nameof(instance)); 
        }
    }
}

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> : IUpdatingAsyncDbDataAdapter
    {
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

        protected virtual async Task<Int32> FillAsync( DataTable[] dataTables, IDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
		    if (dataTables == null || dataTables.Length == 0 || dataTables[0] == null) throw ADP.FillRequires("dataTable");
		    if (dataReader == null) throw ADP.FillRequires("dataReader");
		    if (1 < dataTables.Length && (startRecord != 0 || maxRecords != 0)) throw ADP.NotSupported();

		    int result = 0;
		    bool flag = false;
		    DataSet dataSet = dataTables[0].DataSet;
		    try
		    {
			    if (dataSet != null)
			    {
				    flag = dataSet.EnforceConstraints;
				    dataSet.EnforceConstraints = false;
			    }
			    for (int i = 0; i < dataTables.Length && !dataReader.IsClosed; i++)
			    {
				    DataReaderContainer dataReaderContainer = DataReaderContainer.Create(dataReader, ReturnProviderSpecificTypes);
				    if (dataReaderContainer.FieldCount <= 0)
				    {
					    if (i != 0)
					    {
						    continue;
					    }
					    bool flag2;
					    do
					    {
						    flag2 = FillNextResult(dataReaderContainer);
					    }
					    while (flag2 && dataReaderContainer.FieldCount <= 0);
					    if (!flag2)
					    {
						    break;
					    }
				    }
				    if (0 >= i || FillNextResult(dataReaderContainer))
				    {
					    int num = FillFromReader(null, dataTables[i], null, dataReaderContainer, startRecord, maxRecords, null, null);
					    if (i == 0)
					    {
						    result = num;
					    }
					    continue;
				    }
				    break;
			    }
		    }
		    catch (ConstraintException)
		    {
			    flag = false;
			    throw;
		    }
		    finally
		    {
			    if (flag)
			    {
				    dataSet.EnforceConstraints = true;
			    }
		    }
		    return result;
        }

        protected virtual async Task<Int32> FillAsync( DataSet dataSet, string srcTable, IDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            if (dataSet == null)
		    {
			    throw ADP.FillRequires("dataSet");
		    }
		    if (ADP.IsEmpty(srcTable))
		    {
			    throw ADP.FillRequiresSourceTableName("srcTable");
		    }
		    if (dataReader == null)
		    {
			    throw ADP.FillRequires("dataReader");
		    }
		    if (startRecord < 0)
		    {
			    throw ADP.InvalidStartRecord("startRecord", startRecord);
		    }
		    if (maxRecords < 0)
		    {
			    throw ADP.InvalidMaxRecords("maxRecords", maxRecords);
		    }
		    if (dataReader.IsClosed)
		    {
			    return 0;
		    }

		    DataReaderContainer dataReader2 = DataReaderContainer.Create(dataReader, ReturnProviderSpecificTypes);
		    return FillFromReader(dataSet, null, srcTable, dataReader2, startRecord, maxRecords, null, null);
        }

        //

        internal async Task<Int32> FillFromReaderAsync( DataSet dataset, DataTable datatable, string srcTable, DataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken )
        {
	        int result = 0;
	        int num = 0;
	        do
	        {
		        if (0 >= dataReader.FieldCount)
		        {
			        continue;
		        }
		        SchemaMapping schemaMapping = FillMapping(dataset, datatable, srcTable, dataReader, num, parentChapterColumn, parentChapterValue);
		        num++;
		        if (schemaMapping == null || schemaMapping.DataValues == null || schemaMapping.DataTable == null)
		        {
			        continue;
		        }
		        schemaMapping.DataTable.BeginLoadData();
		        try
		        {
			        if (1 == num && (0 < startRecord || 0 < maxRecords))
			        {
				        result = FillLoadDataRowChunk(schemaMapping, startRecord, maxRecords);
			        }
			        else
			        {
				        int num2 = FillLoadDataRow(schemaMapping);
				        if (1 == num)
				        {
					        result = num2;
				        }
			        }
		        }
		        finally
		        {
			        schemaMapping.DataTable.EndLoadData();
		        }
		        if (datatable != null)
		        {
			        break;
		        }
	        }
	        while (FillNextResult(dataReader));
	        return result;
        }

        private static readonly MethodInfo _fillMapping = Reflection.GetInstanceMethod<TDbDataAdapter>( name: nameof(FillMapping), typeof(DataSet), typeof(DataTable), typeof(String), typeof(DataReaderContainer), typeof(Int32), typeof(DataColumn), typeof(Object) );

        private SchemaMapping2 FillMapping(DataSet dataset, DataTable datatable, string srcTable, DataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {

        }
    }
}
