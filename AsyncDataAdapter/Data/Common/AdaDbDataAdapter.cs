using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    using static AsyncDataReaderConnectionMethods;

    public abstract class AdaDbDataAdapter : AdaDataAdapter, ICloneable, IUpdatedRowOptions, IBatchingAdapter, ICanUpdateAsync
    {
        public const string DefaultSourceTableName = "Table";

        private CommandBehavior _fillCommandBehavior;
        
        private readonly IBatchingAdapter batchingAdapter;

        /// <summary>Normal constructor.</summary>
        protected AdaDbDataAdapter( IBatchingAdapter batchingAdapter )
            : base()
        {
            this.batchingAdapter = batchingAdapter;
        }

        /// <summary>Clone constructor.</summary>
        protected AdaDbDataAdapter( IBatchingAdapter batchingAdapter, AdaDbDataAdapter cloneFrom )
            : base( cloneFrom )
        {
            this.SelectCommand = CloneCommand( cloneFrom.SelectCommand );
            this.InsertCommand = CloneCommand( cloneFrom.InsertCommand );
            this.UpdateCommand = CloneCommand( cloneFrom.UpdateCommand );
            this.DeleteCommand = CloneCommand( cloneFrom.DeleteCommand );

            this.batchingAdapter = batchingAdapter;
        }

        #region Properties

        protected internal CommandBehavior FillCommandBehavior
        {
            get
            {
                return (this._fillCommandBehavior | CommandBehavior.SequentialAccess);
            }
            set
            {
                // setting |= SchemaOnly;       /* similar to FillSchema (which also uses KeyInfo) */
                // setting |= KeyInfo;          /* same as MissingSchemaAction.AddWithKey */
                // setting |= SequentialAccess; /* required and always present */
                // setting |= CloseConnection;  /* close connection regardless of start condition */
                this._fillCommandBehavior = (value | CommandBehavior.SequentialAccess);
                //Bid.Trace("<comm.DbDataAdapter.set_FillCommandBehavior|API> %d#, %d{ds.CommandBehavior}\n", (int)value);
            }
        }

        public DbCommand DeleteCommand { get; set; }

        public DbCommand InsertCommand { get; set; }

        public DbCommand SelectCommand { get; set; }

        public DbCommand UpdateCommand { get; set; }

        [DefaultValue(1)]
        public virtual int UpdateBatchSize
        {
            get => 1;
            set
            {
                if(value != 1)
                {
                    throw new NotSupportedException(message: nameof(this.UpdateBatchSize) + " can only be set to a value of 1.");
                }
            }
        }

        private MissingMappingAction UpdateMappingAction
        {
            get
            {
                if (MissingMappingAction.Passthrough == this.MissingMappingAction)
                {
                    return MissingMappingAction.Passthrough;
                }
                return MissingMappingAction.Error;
            }
        }

        private MissingSchemaAction UpdateSchemaAction
        {
            get
            {
                MissingSchemaAction action = this.MissingSchemaAction;
                if ((MissingSchemaAction.Add == action) || (MissingSchemaAction.AddWithKey == action))
                {
                    return MissingSchemaAction.Ignore;
                }
                return MissingSchemaAction.Error;
            }
        }

        #endregion

        #region IBatchingAdapter

        MissingMappingAction IBatchingAdapter.UpdateMappingAction                                                                            => this.batchingAdapter.UpdateMappingAction;
        MissingSchemaAction  IBatchingAdapter.UpdateSchemaAction                                                                             => this.batchingAdapter.UpdateSchemaAction;
        int                  IBatchingAdapter.AddToBatch(DbCommand command)                                                                  => this.batchingAdapter.AddToBatch(command);
        void                 IBatchingAdapter.ClearBatch()                                                                                   => this.batchingAdapter.ClearBatch();
        Task<int>            IBatchingAdapter.ExecuteBatchAsync(CancellationToken cancellationToken)                                         => this.batchingAdapter.ExecuteBatchAsync(cancellationToken);
        void                 IBatchingAdapter.TerminateBatching()                                                                            => this.batchingAdapter.TerminateBatching();
        IDataParameter       IBatchingAdapter.GetBatchedParameter(int commandIdentifier, int parameterIndex)                                 => this.batchingAdapter.GetBatchedParameter(commandIdentifier, parameterIndex);
        bool                 IBatchingAdapter.GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error) => this.batchingAdapter.GetBatchedRecordsAffected(commandIdentifier, out recordsAffected, out error);
        void                 IBatchingAdapter.InitializeBatching()                                                                           => this.batchingAdapter.InitializeBatching();

        #endregion

        #region ICanUpdateAsync

        void ICanUpdateAsync.OnRowUpdating( RowUpdatingEventArgs e ) => this.OnRowUpdating( e );
        void ICanUpdateAsync.OnRowUpdated ( RowUpdatedEventArgs  e ) => this.OnRowUpdated( e );

        DbConnection ICanUpdateAsync.GetConnection() => this.UpdateCommand?.Connection;

        RowUpdatingEventArgs ICanUpdateAsync.CreateRowUpdatingEvent( DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping ) => this.CreateRowUpdatingEvent( dataRow, command, statementType, tableMapping );
        RowUpdatedEventArgs  ICanUpdateAsync.CreateRowUpdatedEvent ( DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping ) => this.CreateRowUpdatedEvent ( dataRow, command, statementType, tableMapping );

        void ICanUpdateAsync.UpdatingRowStatusErrors(RowUpdatingEventArgs e, DataRow row                                      ) => this.UpdatingRowStatusErrors( e, row );
        int  ICanUpdateAsync.UpdatedRowStatus       (RowUpdatedEventArgs e, BatchCommandInfo[] batchCommands, int commandCount) => this.UpdatedRowStatus( e, batchCommands, commandCount );

        Task ICanUpdateAsync.UpdateRowExecuteAsync(RowUpdatedEventArgs rowUpdatedEvent, DbCommand dataCommand, StatementType cmdIndex, CancellationToken cancellationToken)
        {
            return this.UpdateRowExecuteAsync( rowUpdatedEvent, dataCommand, cmdIndex, cancellationToken );
        }

        Task<ConnectionState> ICanUpdateAsync.UpdateConnectionOpenAsync(DbConnection connection, StatementType statementType, DbConnection[] connections, ConnectionState[] connectionStates, bool useSelectConnectionState, CancellationToken cancellationToken)
        {
            return AsyncDataReaderUpdateMethods.UpdateConnectionOpenAsync( connection, statementType, connections, connectionStates, useSelectConnectionState, cancellationToken );
        }

        #endregion

        /// <summary>Implementations must return a new instance of the same type.</summary>
        public abstract Object Clone();

        private static DbCommand CloneCommand(DbCommand command)
        {
            if( command is ICloneable clonableCommand )
            {
                Object clonedCmdObj = clonableCommand.Clone();
                if( clonedCmdObj is DbCommand clonedCmd )
                {
                    return clonedCmd;
                }
                else
                {
                    string msg = "Expected Clone() to return a DbCommand instance, but instead encountered " + ( clonedCmdObj is null ? "null" : clonedCmdObj.GetType().AssemblyQualifiedName );
                    throw new InvalidOperationException(message: msg);
                }
            }

            return null;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // release mananged objects
                this.SelectCommand = null;
                this.InsertCommand = null;
                this.UpdateCommand = null;
                this.DeleteCommand = null;
            }

            base.Dispose(disposing); // notify base classes
        }

        #region FillSchema

        public async Task<DataTable> FillSchemaAsync( DataTable dataTable, SchemaType schemaType, CancellationToken cancellationToken )
        {
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;
            return await this.FillSchemaAsync(dataTable, schemaType, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false); // MDAC 67666
        }

        public override async Task<DataTable[]> FillSchemaAsync( DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken )
        {
            DbCommand command = this.SelectCommand;

            // design-time support:
            bool isDesignModeWithoutACommand = this.DesignMode && ( command is null || command.Connection is null || String.IsNullOrEmpty( command.CommandText ) );
            if (isDesignModeWithoutACommand)
            {
                return Array.Empty<DataTable>();
            }

            CommandBehavior cmdBehavior = this.FillCommandBehavior;

            return await this.FillSchemaAsync( dataSet, schemaType, command, AdaDbDataAdapter.DefaultSourceTableName, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        public async Task<DataTable[]> FillSchemaAsync( DataSet dataSet, SchemaType schemaType, string srcTable, CancellationToken cancellationToken )
        {
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;
            return await this.FillSchemaAsync(dataSet, schemaType, selectCmd, srcTable, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        protected virtual async Task<DataTable[]> FillSchemaAsync( DataSet dataSet, SchemaType schemaType, DbCommand command, string srcTable, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            if (null == dataSet) throw new ArgumentNullException(nameof(dataSet));
            if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType)) throw ADP.InvalidSchemaType(schemaType);
            if (string.IsNullOrEmpty(srcTable)) throw ADP.FillSchemaRequiresSourceTableName("srcTable");
            if (null == command) throw ADP.MissingSelectCommand(method: "FillSchema");

            return (DataTable[]) await this.FillSchemaInternalAsync(dataSet, null, schemaType, command, srcTable, behavior, cancellationToken ).ConfigureAwait(false);
        }

        protected virtual async Task<DataTable> FillSchemaAsync( DataTable dataTable, SchemaType schemaType, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            if (null == dataTable) throw new ArgumentNullException(nameof(dataTable));
            if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType)) throw ADP.InvalidSchemaType(schemaType);
            if (null == command) throw ADP.MissingSelectCommand("FillSchema");

            string srcTableName = dataTable.TableName;
            int index = this.IndexOfDataSetTable( srcTableName );
            if (-1 != index)
            {
                srcTableName = this.TableMappings[index].SourceTable;
            }

            DataTable[] singleTable = await this.FillSchemaInternalAsync( null, dataTable, schemaType, command, srcTableName, behavior | CommandBehavior.SingleResult, cancellationToken ).ConfigureAwait(false);
            if( singleTable is DataTable[] arr && arr.Length == 1 )
            {
                return singleTable[0];
            }
            else
            {
                return null;
            }
        }

        private async Task<DataTable[]> FillSchemaInternalAsync(DataSet dataset, DataTable datatable, SchemaType schemaType, DbCommand command, string srcTable, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            DbConnection activeConnection = AdaDbDataAdapter.GetConnection3(command, "FillSchema");
            ConnectionState originalState = ConnectionState.Open;

            try
            {
                originalState = await QuietOpenAsync( activeConnection, cancellationToken ).ConfigureAwait(false);
                using (DbDataReader dataReader = await command.ExecuteReaderAsync( behavior | CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo, cancellationToken ).ConfigureAwait(false) )
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
                QuietClose( activeConnection, originalState);
            }
        }

        #endregion

        #region FillAsync

            #region FillAsync(DataSet)

        public override async Task<int> FillAsync( DataSet dataSet, CancellationToken cancellationToken = default )
        {
            // delegate to Fill4
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;

            return await this.FillAsync( dataSet, 0, 0, AdaDbDataAdapter.DefaultSourceTableName, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        public async Task<int> FillAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken )
        {
            // delegate to Fill4
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;

            return await this.FillAsync( dataSet, 0, 0, srcTable, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        public async Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, CancellationToken cancellationToken )
        {
            // delegate to Fill4
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;

            return await this.FillAsync( dataSet, startRecord, maxRecords, srcTable, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        protected virtual async Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            if (null == dataSet) throw ADP.FillRequires("dataSet");
            if (startRecord < 0) throw ADP.InvalidStartRecord("startRecord", startRecord);
            if (maxRecords < 0) throw ADP.InvalidMaxRecords("maxRecords", maxRecords);
            if (string.IsNullOrEmpty(srcTable)) throw ADP.FillRequiresSourceTableName("srcTable");
            if (null == command) throw ADP.MissingSelectCommand("Fill");

            return await this.FillInternalAsync( dataSet, null, startRecord, maxRecords, srcTable, command, behavior, cancellationToken ).ConfigureAwait(false);
        }

            #endregion

            #region FillAsync(DataTable)

        public async Task<int> FillAsync(DataTable dataTable, CancellationToken cancellationToken = default )
        {
            // delegate to Fill8
            DataTable[] dataTables = new DataTable[1] { dataTable };
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;
            return await this.FillAsync(dataTables, 0, 0, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

//      public async Task<int> FillAsync(int startRecord, int maxRecords, params DataTable[] dataTables)
        public async Task<int> FillAsync(int startRecord, int maxRecords, CancellationToken cancellationToken, params DataTable[] dataTables)
        {
            // delegate to Fill8
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;
            return await this.FillAsync(dataTables, startRecord, maxRecords, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        protected virtual async Task<int> FillAsync(DataTable dataTable, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            // delegate to Fill8
            DataTable[] dataTables = new DataTable[1] { dataTable };
            return await this.FillAsync(dataTables, 0, 0, command, behavior, cancellationToken ).ConfigureAwait(false);
        }

            #endregion

        protected virtual async Task<int> FillAsync(DataTable[] dataTables, int startRecord, int maxRecords, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            if ((null == dataTables) || (0 == dataTables.Length) || (null == dataTables[0])) throw ADP.FillRequires("dataTable");
            if (startRecord < 0) throw ADP.InvalidStartRecord("startRecord", startRecord);
            if (maxRecords < 0) throw ADP.InvalidMaxRecords("maxRecords", maxRecords);
            if ((1 < dataTables.Length) && ((0 != startRecord) || (0 != maxRecords))) throw ADP.OnlyOneTableForStartRecordOrMaxRecords();
            if (null == command) throw ADP.MissingSelectCommand("Fill");
            
            //

            if (1 == dataTables.Length)
            {
                behavior |= CommandBehavior.SingleResult;
            }

            return await this.FillInternalAsync(null, dataTables, startRecord, maxRecords, null, command, behavior, cancellationToken ).ConfigureAwait(false);
        }

        private async Task<int> FillInternalAsync( DataSet dataset, DataTable[] datatables, int startRecord, int maxRecords, string srcTable, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            int rowsAddedToDataSet = 0;
            bool restoreNullConnection = (null == command.Connection);
            try
            {
                DbConnection activeConnection = AdaDbDataAdapter.GetConnection3(command, "Fill");
                ConnectionState originalState = ConnectionState.Open;

                // the default is MissingSchemaAction.Add, the user must explicitly
                // set MisingSchemaAction.AddWithKey to get key information back in the dataset
                if (MissingSchemaAction.AddWithKey == this.MissingSchemaAction)
                {
                    behavior |= CommandBehavior.KeyInfo;
                }

                try
                {
                    originalState = await QuietOpenAsync( activeConnection, cancellationToken ).ConfigureAwait(false);
                    behavior |= CommandBehavior.SequentialAccess;

                    using( DbDataReader dbDataReader = await command.ExecuteReaderAsync( behavior, cancellationToken ).ConfigureAwait(false) )
                    {
                        if (datatables != null)
                        {
                            // delegate to next set of protected Fill methods
                            rowsAddedToDataSet = await this.FillAsync( datatables, dbDataReader, startRecord, maxRecords, cancellationToken ).ConfigureAwait(false);
                        }
                        else
                        {
                            rowsAddedToDataSet = await this.FillAsync( dataset, srcTable, dbDataReader, startRecord, maxRecords, cancellationToken ).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    QuietClose( activeConnection, originalState );
                }
            }
            finally
            {
                if (restoreNullConnection)
                {
                    command.Transaction = null;
                    command.Connection = null;
                }
            }
            return rowsAddedToDataSet;
        }

        #endregion

        #region Batching

        /// <summary>Called to add a single command to the batch of commands that need to be executed as a batch, when batch updates are requested.  It must return an identifier that can be used to identify the command to GetBatchedParameter later.</summary>
        protected abstract int AddToBatch(DbCommand command);

        /// <summary>Called when batch updates are requested to clear out the contents of the batch, whether or not it's been executed.</summary>
        protected abstract void ClearBatch();

        /// <summary>Called to execute the batched update command, returns the number of rows affected, just as ExecuteNonQuery would.</summary>
        protected abstract Task<int> ExecuteBatchAsync( CancellationToken cancellationToken );

        /// <summary>Called when batch updates are requested to cleanup after a batch update has been completed.</summary>
        protected abstract void TerminateBatching();

        /// <summary>Called to retrieve a parameter from a specific bached command, the first argument is the value that was returned by AddToBatch when it was called for the command.</summary>
        protected abstract IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex);

        protected virtual bool GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error)
        { // SQLBU 412467
            // Called to retrieve the records affected from a specific batched command,
            // first argument is the value that was returned by AddToBatch when it
            // was called for the command.

            // default implementation always returns 1, derived classes override for otherwise
            // otherwise DbConcurrencyException will only be thrown if sum of all records in batch is 0

            // return 0 to cause Update to throw DbConcurrencyException
            recordsAffected = 1;
            error = null;
            return true;
        }

        /// <summary>Called when batch updates are requested to prepare for processing of a batch of commands.</summary>
        protected abstract void InitializeBatching();

        #endregion

        [EditorBrowsableAttribute(EditorBrowsableState.Advanced)] // MDAC 69508
        public override IDataParameter[] GetFillParameters()
        {
            if( this.SelectCommand is DbCommand cmd && cmd.Parameters is DbParameterCollection pList && pList.Count > 0 )
            {
                IDataParameter[] array = new IDataParameter[pList.Count];
                pList.CopyTo( array, index: 0 );
                return array;
            }

            return Array.Empty<IDataParameter>();
        }

        internal DataTableMapping GetTableMapping(DataTable dataTable)
        {
            DataTableMapping tableMapping = null;
            int index = this.IndexOfDataSetTable(dataTable.TableName);
            if (-1 != index)
            {
                tableMapping = this.TableMappings[index];
            }

            if (null == tableMapping)
            {
                if (this.MissingMappingAction == MissingMappingAction.Error)
                {
                    throw ADP.MissingTableMappingDestination(dataTable.TableName);
                }

                tableMapping = new DataTableMapping(dataTable.TableName, dataTable.TableName);
            }

            return tableMapping;
        }

        private void ParameterInput(IDataParameterCollection parameters, StatementType typeIndex, DataRow row, DataTableMapping mappings)
        {
            ParameterMethods.ParameterInput( this.UpdateMappingAction, this.UpdateSchemaAction, parameters, typeIndex, row, mappings );
        }

        private void ParameterOutput(IDataParameterCollection parameters, DataRow row, DataTableMapping mappings)
        {
            ParameterMethods.ParameterOutput( this.UpdateMappingAction, this.UpdateSchemaAction, parameters, row, mappings );
        }

        #region UpdateAsync

        protected virtual RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            return new RowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
        }

        protected virtual RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            return new RowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
        }

        protected virtual void OnRowUpdated(RowUpdatedEventArgs value)
        {
        }

        protected virtual void OnRowUpdating(RowUpdatingEventArgs value)
        {
        }

        public override Task<int> UpdateAsync( DataSet dataSet, CancellationToken cancellationToken )
        {
            if (!TableMappings.Contains(DbDataAdapter.DefaultSourceTableName))
            {
                string msg = string.Format("Update unable to find TableMapping['{0}'] or DataTable '{0}'.", DbDataAdapter.DefaultSourceTableName);
                throw new InvalidOperationException(msg);
            }

            return this.UpdateAsync( dataSet, srcTable: AdaDbDataAdapter.DefaultSourceTableName, cancellationToken );
        }

        public async Task<int> UpdateAsync(DataRow[] dataRows, CancellationToken cancellationToken)
        {
            int rowsAffected = 0;
            if (null == dataRows)
            {
                throw new ArgumentNullException(nameof(dataRows));
            }
            else if (0 != dataRows.Length)
            {
                DataTable dataTable = null;
                for (int i = 0; i < dataRows.Length; ++i)
                {
                    if ((null != dataRows[i]) && (dataTable != dataRows[i].Table))
                    {
                        if (null != dataTable) throw new ArgumentException(string.Format("DataRow[{0}] is from a different DataTable than DataRow[0].", i));
                        dataTable = dataRows[i].Table;
                    }
                }
                if (null != dataTable)
                {
                    DataTableMapping tableMapping = this.GetTableMapping(dataTable);
                    rowsAffected = await this.UpdateAsync( dataRows, tableMapping, cancellationToken ).ConfigureAwait(false);
                }
            }
            return rowsAffected;
        }

        public async Task<int> UpdateAsync(DataTable dataTable, CancellationToken cancellationToken)
        {
            {
                if (dataTable is null) throw new ArgumentNullException(nameof(dataTable));

                DataTableMapping tableMapping = null;
                int index = this.IndexOfDataSetTable(dataTable.TableName);
                if (-1 != index)
                {
                    tableMapping = this.TableMappings[index];
                }

                if (null == tableMapping)
                {
                    if (MissingMappingAction.Error == this.MissingMappingAction)
                    {
                        throw ADP.MissingTableMappingDestination(dataTable.TableName);
                    }
                    tableMapping = new DataTableMapping(AdaDbDataAdapter.DefaultSourceTableName, dataTable.TableName);
                }

                return await this.UpdateFromDataTableAsync( dataTable, tableMapping, cancellationToken ).ConfigureAwait(false);
            }
        }

        public async Task<int> UpdateAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken)
        {
            if (dataSet is null) throw new ArgumentNullException(nameof(dataSet));
            if (srcTable is null) throw new ArgumentNullException(nameof(srcTable));
            if (string.IsNullOrEmpty(srcTable)) throw new ArgumentException(message: "Update: expected a non-empty SourceTable name.", paramName: nameof(srcTable));

            int rowsAffected = 0;

            DataTableMapping tableMapping = this.GetTableMappingBySchemaAction(srcTable, srcTable, this.UpdateMappingAction);
            Debug.Assert(null != tableMapping, "null TableMapping when MissingMappingAction.Error");

            // the ad-hoc scenario of no dataTable just returns
            // ad-hoc scenario is defined as MissingSchemaAction.Add or MissingSchemaAction.Ignore
            MissingSchemaAction schemaAction = this.UpdateSchemaAction;
            DataTable dataTable = tableMapping.GetDataTableBySchemaAction(dataSet, schemaAction);
            if (null != dataTable)
            {
                rowsAffected = await this.UpdateFromDataTableAsync(dataTable, tableMapping, cancellationToken ).ConfigureAwait(false);
            }
            else if ( (this.TableMappings?.Count ?? 0) == 0 || (-1 == this.TableMappings.IndexOf(tableMapping)))
            {
                //throw error since the user didn't explicitly map this tableName to Ignore.
                throw new InvalidOperationException(string.Format("Update unable to find TableMapping['{0}'] or DataTable '{0}'.", srcTable));
            }
            return rowsAffected;
        }

        protected virtual Task<int> UpdateAsync(DataRow[] dataRows, DataTableMapping tableMapping, CancellationToken cancellationToken)
        {
            return AsyncDataReaderUpdateMethods.UpdateAsync( this, dataRows, tableMapping, cancellationToken );
        }

        private async Task UpdateBatchExecuteAsync(BatchCommandInfo[] batchCommands, int commandCount, RowUpdatedEventArgs rowUpdatedEvent, CancellationToken cancellationToken )
        {
            try
            {
                // the batch execution may succeed, partially succeed and throw an exception (or not), or totally fail
                int recordsAffected = await this.ExecuteBatchAsync( cancellationToken );
                rowUpdatedEvent.AdapterInit_(recordsAffected);
            }
            catch (DbException e)
            {
                // an exception was thrown be but some part of the batch may have been succesfull
                rowUpdatedEvent.Errors = e;
                rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
            }

            this.AfterUpdateBatchExecute( batchCommands, commandCount, rowUpdatedEvent );
        }

        private void AfterUpdateBatchExecute(BatchCommandInfo[] batchCommands, int commandCount, RowUpdatedEventArgs rowUpdatedEvent )
        {
            MissingMappingAction missingMapping = this.UpdateMappingAction;
            MissingSchemaAction  missingSchema  = this.UpdateSchemaAction;

            int checkRecordsAffected = 0;
            bool hasConcurrencyViolation = false;
            List<DataRow> rows = null;

            // walk through the batch to build the sum of recordsAffected
            //      determine possible indivdual messages per datarow
            //      determine possible concurrency violations per datarow
            //      map output parameters to the datarow
            for (int bc = 0; bc < commandCount; ++bc)
            {
                BatchCommandInfo batchCommand = batchCommands[bc];
                StatementType statementType = batchCommand.StatementType;

                // default implementation always returns 1, derived classes must override
                // otherwise DbConcurrencyException will only be thrown if sum of all records in batch is 0
                if (this.GetBatchedRecordsAffected(batchCommand.CommandIdentifier, out int rowAffected, error: out batchCommands[bc].Errors))
                {
                    batchCommands[bc].RecordsAffected = rowAffected;
                }

                if ((null == batchCommands[bc].Errors) && batchCommands[bc].RecordsAffected.HasValue)
                {
                    // determine possible concurrency violations per datarow
                    if ((StatementType.Update == statementType) || (StatementType.Delete == statementType))
                    {
                        checkRecordsAffected++;
                        if (0 == rowAffected)
                        {
                            if (null == rows)
                            {
                                rows = new List<DataRow>();
                            }
                            batchCommands[bc].Errors = ADP.UpdateConcurrencyViolation(batchCommands[bc].StatementType, 0, 1, new DataRow[] { rowUpdatedEvent.GetRow_(bc) });
                            hasConcurrencyViolation = true;
                            rows.Add(rowUpdatedEvent.GetRow_(bc));
                        }
                    }

                    // map output parameters to the datarow
                    if (((StatementType.Insert == statementType) || (StatementType.Update == statementType))
                        && (0 != (UpdateRowSource.OutputParameters & batchCommand.UpdatedRowSource)) && (0 != rowAffected))  // MDAC 71174
                    {
                        if (StatementType.Insert == statementType)
                        {
                            // AcceptChanges for 'added' rows so backend generated keys that are returned
                            // propagte into the datatable correctly.
                            rowUpdatedEvent.GetRow_(bc).AcceptChanges();
                        }

                        for (int i = 0; i < batchCommand.ParameterCount; ++i)
                        {
                            IDataParameter parameter = this.GetBatchedParameter(batchCommand.CommandIdentifier, i);
                            ParameterMethods.ParameterOutput( parameter, batchCommand.Row, rowUpdatedEvent.TableMapping, missingMapping, missingSchema );
                        }
                    }
                }
            }

            if (null == rowUpdatedEvent.Errors)
            {
                // Only error if RecordsAffect == 0, not -1.  A value of -1 means no count was received from server,
                // do not error in that situation (means 'set nocount on' was executed on server).
                if (UpdateStatus.Continue == rowUpdatedEvent.Status)
                {
                    if ((0 < checkRecordsAffected) && ((0 == rowUpdatedEvent.RecordsAffected) || hasConcurrencyViolation))
                    {
                        // bug50526, an exception if no records affected and attempted an Update/Delete
                        Debug.Assert(null == rowUpdatedEvent.Errors, "Continue - but contains an exception");
                        DataRow[] rowsInError = (null != rows) ? rows.ToArray() : rowUpdatedEvent.GetRows_();
                        rowUpdatedEvent.Errors = ADP.UpdateConcurrencyViolation(StatementType.Batch, commandCount - rowsInError.Length, commandCount, rowsInError); // MDAC 55735
                        rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
                    }
                }
            }
        }

        private Task<int> UpdateFromDataTableAsync(DataTable dataTable, DataTableMapping tableMapping, CancellationToken cancellationToken )
        {
            return AsyncDataReaderUpdateMethods.UpdateFromDataTableAsync( this, dataTable, tableMapping, cancellationToken );
        }

        private Task UpdateRowExecuteAsync( RowUpdatedEventArgs rowUpdatedEvent, DbCommand dataCommand, StatementType cmdIndex, CancellationToken cancellationToken )
        {
            return AsyncDataReaderUpdateMethods.UpdateRowExecuteAsync( this, this.ReturnProviderSpecificTypes, rowUpdatedEvent, dataCommand, cmdIndex, cancellationToken );
        }

        private int UpdatedRowStatus( RowUpdatedEventArgs rowUpdatedEvent, BatchCommandInfo[] batchCommands, int commandCount )
        {
            return AsyncDataReaderBatchExecuteMethods.UpdatedRowStatus( this, rowUpdatedEvent, batchCommands, commandCount );
        }

        private int UpdatedRowStatusContinue( BatchCommandInfo[] batchCommands, int commandCount )
        {
            return AsyncDataReaderBatchExecuteMethods.UpdatedRowStatusContinue( this, batchCommands, commandCount );
        }

        private int UpdatedRowStatusErrors( RowUpdatedEventArgs rowUpdatedEvent, BatchCommandInfo[] batchCommands, int commandCount )
        {
            return AsyncDataReaderBatchExecuteMethods.UpdatedRowStatusErrors( this, rowUpdatedEvent, batchCommands, commandCount );
        }

        private int UpdatedRowStatusSkip( BatchCommandInfo[] batchCommands, int commandCount )
        {
            return AsyncDataReaderBatchExecuteMethods.UpdatedRowStatusSkip( batchCommands, commandCount );
        }

        private void UpdatingRowStatusErrors( RowUpdatingEventArgs rowUpdatedEvent, DataRow dataRow )
        {
            AsyncDataReaderUpdateMethods.UpdatingRowStatusErrors( continueUpdateOnError: this.ContinueUpdateOnError, rowUpdatedEvent, dataRow );
        }

        #endregion

        #region GetConnection

        private static DbConnection GetConnection1(AdaDbDataAdapter adapter)
        {
            DbCommand command = adapter.SelectCommand;
            if (null == command)
            {
                command = adapter.InsertCommand;
                if (null == command)
                {
                    command = adapter.UpdateCommand;
                    if (null == command)
                    {
                        command = adapter.DeleteCommand;
                    }
                }
            }
            DbConnection connection = null;
            if (null != command)
            {
                connection = command.Connection;
            }
            if (null == connection)
            {
                throw ADP.UpdateConnectionRequired(StatementType.Batch, false);
            }
            return connection;
        }

        private static DbConnection GetConnection3(DbCommand command, string method)
        {
            Debug.Assert(null != command, "GetConnection3: null command");
            Debug.Assert(!string.IsNullOrEmpty(method), "missing method name");
            DbConnection connection = command.Connection;
            if (connection is null)
            {
                string message = method + " requires a non-null " + nameof(DbConnection) + " reference in the " + nameof(DbCommand) + "." + nameof(DbCommand.Connection) + " property.";
                throw new InvalidOperationException(message);
            }
            return connection;
        }

        #endregion

        
    }
}
