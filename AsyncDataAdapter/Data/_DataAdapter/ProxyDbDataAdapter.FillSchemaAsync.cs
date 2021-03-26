using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader>
    {
        public Task<DataTable> FillSchemaAsync( DataTable dataTable, SchemaType schemaType, CancellationToken cancellationToken )
        {
            TDbCommand selectCommand = this.SelectCommand;
            CommandBehavior fillCommandBehavior = this.FillCommandBehavior;

            return this.FillSchemaAsync( dataTable, schemaType, selectCommand, fillCommandBehavior, cancellationToken );
        }

        public Task<DataTable[]> FillSchemaAsync( DataSet dataSet, SchemaType schemaType, string srcTable, CancellationToken cancellationToken )
        {
            TDbCommand selectCommand = this.SelectCommand;
            CommandBehavior fillCommandBehavior = this.FillCommandBehavior;

            return this.FillSchemaAsync( dataSet, schemaType, selectCommand, srcTable, fillCommandBehavior, cancellationToken );
        }

        protected virtual async Task<DataTable[]> FillSchemaAsync( DataSet dataSet, SchemaType schemaType, TDbCommand command, string srcTable, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            if (dataSet is null) throw new ArgumentNullException(nameof(dataSet));
            if (SchemaType.Source != schemaType && SchemaType.Mapped != schemaType) throw ADP.InvalidSchemaType(schemaType);
            if (String.IsNullOrEmpty(srcTable)) throw ADP.FillSchemaRequiresSourceTableName("srcTable");
            if (command == null) throw ADP.MissingSelectCommand("FillSchema");
            
            //

            var arr = await this.FillSchemaInternalAsync( dataSet, null, schemaType, command, srcTable, behavior, cancellationToken ).ConfigureAwait(false);

            return (DataTable[])arr;
        }

        protected virtual async Task<DataTable> FillSchemaAsync( DataTable dataTable, SchemaType schemaType, TDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            if (dataTable is null) throw new ArgumentNullException(nameof(dataTable));
            if (SchemaType.Source != schemaType && SchemaType.Mapped != schemaType) throw ADP.InvalidSchemaType(schemaType);
            if (command == null) throw ADP.MissingSelectCommand("FillSchema");

            string text = dataTable.TableName;

            IAdaSchemaMappingAdapter self = this;

            int num = self.IndexOfDataSetTable( text );
            if (-1 != num)
            {
                text = base.TableMappings[num].SourceTable;
            }

            var table = await FillSchemaInternalAsync( null, dataTable, schemaType, command, text, behavior | CommandBehavior.SingleResult, cancellationToken ).ConfigureAwait(false);
            return (DataTable)table;
        }

        private async Task<Object> FillSchemaInternalAsync( DataSet dataset, DataTable datatable, SchemaType schemaType, TDbCommand command, string srcTable, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            TDbConnection   connection    = GetConnection( command );
		    ConnectionState originalState = ConnectionState.Open;

            behavior = CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo;

            try
            {
                originalState = await QuietOpenAsync( connection, cancellationToken ).ConfigureAwait(false);
                using( TDbDataReader dataReader = await ExecuteReaderAsync( command, behavior, cancellationToken ).ConfigureAwait(false) )
                {
                    if (null != datatable)
                    {
                        // delegate to next set of protected FillSchema methods
                        DataTable singleTable = await this.FillSchemaAsync( datatable, schemaType, dataReader, cancellationToken ).ConfigureAwait(false);
                        return new[] { singleTable };
                    }
                    else
                    {
                        return await this.FillSchemaAsync( dataset, schemaType, srcTable, dataReader, cancellationToken ).ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                QuietClose( connection, originalState);
            }
        }

        protected virtual Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, TDbDataReader dataReader, CancellationToken cancellationToken )
        {
            return AsyncDataReaderMethods.FillSchemaAsync( adapter: this, this.ReturnProviderSpecificTypes, dataSet: dataSet, schemaType, srcTable, dataReader, cancellationToken );
        }

        protected virtual Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, TDbDataReader dataReader, CancellationToken cancellationToken )
        {
            return AsyncDataReaderMethods.FillSchemaAsync( adapter: this, this.ReturnProviderSpecificTypes, dataTable: dataTable, schemaType, dataReader, cancellationToken );
        }
    }
}
