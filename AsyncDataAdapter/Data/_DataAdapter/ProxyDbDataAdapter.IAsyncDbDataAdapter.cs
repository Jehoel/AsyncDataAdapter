using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> : IUpdatingAsyncDbDataAdapter, IBatchingAdapter, ICanUpdateAsync, IUpdatedRowOptions
    {
        #region IAsyncDataReader

        public Task<Int32> FillAsync(DataSet dataSet, CancellationToken cancellationToken = default )
        {
            TDbCommand      selectCommand       = this.SelectCommand;
		    CommandBehavior fillCommandBehavior = this.FillCommandBehavior;

		    return this.FillAsync( dataSet, 0, 0, AdaDbDataAdapter.DefaultSourceTableName, selectCommand, fillCommandBehavior, cancellationToken );
        }

        public async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken = default)
        {
            TDbCommand selectCommand = this.SelectCommand;

		    if ( base.DesignMode && ( selectCommand == null || selectCommand.Connection == null || String.IsNullOrEmpty( selectCommand.CommandText ) ) )
		    {
			    return Array.Empty<DataTable>();
		    }

		    CommandBehavior fillCommandBehavior = FillCommandBehavior;

		    return await this.FillSchemaAsync( dataSet, schemaType, selectCommand, AdaDbDataAdapter.DefaultSourceTableName, fillCommandBehavior, cancellationToken ).ConfigureAwait(false);
        }

        #endregion

        #region IUpdatingAsyncDataAdapter

        public Task<int> UpdateAsync( DataSet dataSet, CancellationToken cancellationToken = default )
        {
            // The original in ReferenceSource would throw an exception due to this guard: `!TableMappings.Contains(DbDataAdapter.DefaultSourceTableName)` // MDAC 59268
            // The comment was left in voloda's copy, but I thought they commented it out themselves as a TODO. Turns out they didn't, whoops.

            return this.UpdateAsync( dataSet, srcTable: AdaDbDataAdapter.DefaultSourceTableName, cancellationToken );
        }

        #endregion

        #region IBatchingAdapter

        MissingMappingAction IBatchingAdapter.UpdateMappingAction                                                                            => this.BatchingAdapter.UpdateMappingAction;
        MissingSchemaAction  IBatchingAdapter.UpdateSchemaAction                                                                             => this.BatchingAdapter.UpdateSchemaAction;
        int                  IBatchingAdapter.AddToBatch(DbCommand command)                                                                  => this.BatchingAdapter.AddToBatch(command);
        void                 IBatchingAdapter.ClearBatch()                                                                                   => this.BatchingAdapter.ClearBatch();
        Task<int>            IBatchingAdapter.ExecuteBatchAsync(CancellationToken cancellationToken)                                         => this.BatchingAdapter.ExecuteBatchAsync(cancellationToken);
        void                 IBatchingAdapter.TerminateBatching()                                                                            => this.BatchingAdapter.TerminateBatching();
        IDataParameter       IBatchingAdapter.GetBatchedParameter(int commandIdentifier, int parameterIndex)                                 => this.BatchingAdapter.GetBatchedParameter(commandIdentifier, parameterIndex);
        bool                 IBatchingAdapter.GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error) => this.BatchingAdapter.GetBatchedRecordsAffected(commandIdentifier, out recordsAffected, out error);
        void                 IBatchingAdapter.InitializeBatching()                                                                           => this.BatchingAdapter.InitializeBatching();

        #endregion

        #region ICanUpdateAsync

        void ICanUpdateAsync.OnRowUpdating( RowUpdatingEventArgs e ) => this.OnRowUpdating( e );
        void ICanUpdateAsync.OnRowUpdated ( RowUpdatedEventArgs  e ) => this.OnRowUpdated( e );

        DbConnection ICanUpdateAsync.GetConnection() => this.UpdateCommand?.Connection;

        RowUpdatingEventArgs ICanUpdateAsync.CreateRowUpdatingEvent( DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping ) => this.CreateRowUpdatingEvent( dataRow, command, statementType, tableMapping );
        RowUpdatedEventArgs  ICanUpdateAsync.CreateRowUpdatedEvent ( DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping ) => this.CreateRowUpdatedEvent ( dataRow, command, statementType, tableMapping );

        void ICanUpdateAsync.UpdatingRowStatusErrors(RowUpdatingEventArgs rowUpdatedEvent, DataRow dataRow)
        {
            AsyncDataReaderUpdateMethods.UpdatingRowStatusErrors( continueUpdateOnError: this.ContinueUpdateOnError, rowUpdatedEvent, dataRow );
        }

        int ICanUpdateAsync.UpdatedRowStatus(RowUpdatedEventArgs rowUpdatedEvent, BatchCommandInfo[] batchCommands, int commandCount)
        {
            return AsyncDataReaderBatchExecuteMethods.UpdatedRowStatus( this, rowUpdatedEvent, batchCommands, commandCount );
        }

        Task ICanUpdateAsync.UpdateRowExecuteAsync(RowUpdatedEventArgs rowUpdatedEvent, DbCommand dataCommand, StatementType cmdIndex, CancellationToken cancellationToken)
        {
            return AsyncDataReaderUpdateMethods.UpdateRowExecuteAsync( this, this.ReturnProviderSpecificTypes, rowUpdatedEvent, dataCommand, cmdIndex, cancellationToken );
        }

        Task<ConnectionState> ICanUpdateAsync.UpdateConnectionOpenAsync(DbConnection connection, StatementType statementType, DbConnection[] connections, ConnectionState[] connectionStates, bool useSelectConnectionState, CancellationToken cancellationToken)
        {
            return AsyncDataReaderUpdateMethods.UpdateConnectionOpenAsync( connection, statementType, connections, connectionStates, useSelectConnectionState, cancellationToken );
        }

        #endregion
    }
}
