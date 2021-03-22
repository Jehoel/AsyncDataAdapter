//------------------------------------------------------------------------------
// <copyright file="DbDataAdapter.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
// <owner current="true" primary="true">[....]</owner>
// <owner current="true" primary="false">[....]</owner>
//------------------------------------------------------------------------------

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
    public abstract class AdaDbDataAdapter : AdaDataAdapter, /* IDbDataAdapter, */ ICloneable
    {
        public const string DefaultSourceTableName = "Table";

        internal static readonly object ParameterValueNonNullValue = 0;
        internal static readonly object ParameterValueNullValue = 1;

        private DbCommand _deleteCommand, _insertCommand, _selectCommand, _updateCommand;

        private CommandBehavior _fillCommandBehavior;

        private struct BatchCommandInfo
        {
            internal int             CommandIdentifier;     // whatever AddToBatch returns, so we can reference the command later in GetBatchedParameter
            internal int             ParameterCount;        // number of parameters on the command, so we know how many to loop over when processing output parameters
            internal DataRow         Row;                   // the row that the command is intended to update
            internal StatementType   StatementType;         // the statement type of the command, needed for accept changes
            internal UpdateRowSource UpdatedRowSource;      // the UpdatedRowSource value from the command, to know whether we need to look for output parameters or not
            internal int?            RecordsAffected;
            internal Exception       Errors;
        }

        protected AdaDbDataAdapter() : base()
        {
        }

        protected AdaDbDataAdapter(AdaDbDataAdapter adapter) : base(adapter)
        { // V1.0.5000
            this.CloneFrom(adapter);
        }

        //[
        //Browsable(false),
        //DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden),
        //]
        //public DbCommand DeleteCommand
        //{ // V1.2.3300
        //    get
        //    {
        //        return (DbCommand)(this.DeleteCommand);
        //    }
        //    set
        //    {
        //        this.DeleteCommand = value;
        //    }
        //}

        public DbCommand DeleteCommand
        { // V1.2.3300
            get
            {
                return _deleteCommand;
            }
            set
            {
                _deleteCommand = value;
            }
        }

        protected internal CommandBehavior FillCommandBehavior
        { // V1.2.3300, MDAC 87511
            get
            {
                //Bid.Trace("<comm.DbDataAdapter.get_FillCommandBehavior|API> %d#\n", ObjectID);
                return (_fillCommandBehavior | CommandBehavior.SequentialAccess);
            }
            set
            {
                // setting |= SchemaOnly;       /* similar to FillSchema (which also uses KeyInfo) */
                // setting |= KeyInfo;          /* same as MissingSchemaAction.AddWithKey */
                // setting |= SequentialAccess; /* required and always present */
                // setting |= CloseConnection;  /* close connection regardless of start condition */
                _fillCommandBehavior = (value | CommandBehavior.SequentialAccess);
                //Bid.Trace("<comm.DbDataAdapter.set_FillCommandBehavior|API> %d#, %d{ds.CommandBehavior}\n", (int)value);
            }
        }

        //[
        //Browsable(false),
        //DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden),
        //]
        //public DbCommand InsertCommand
        //{ // V1.2.3300
        //    get
        //    {
        //        return (DbCommand)(this.InsertCommand);
        //    }
        //    set
        //    {
        //        this.InsertCommand = value;
        //    }
        //}

        public DbCommand InsertCommand
        { // V1.2.3300
            get
            {
                return _insertCommand;
            }
            set
            {
                _insertCommand = value;
            }
        }

        //[
        //Browsable(false),
        //DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden),
        //]
        //public DbCommand SelectCommand
        //{ // V1.2.3300
        //    get
        //    {
        //        return (DbCommand)(this.SelectCommand);
        //    }
        //    set
        //    {
        //        this.SelectCommand = value;
        //    }
        //}

        public DbCommand SelectCommand
        { // V1.2.3300
            get
            {
                return _selectCommand;
            }
            set
            {
                _selectCommand = value;
            }
        }

        [
        DefaultValue(1),
        CategoryAttribute("Settings"),
        DescriptionAttribute("Update batch size"),
        ]
        virtual public int UpdateBatchSize
        {
            get
            {
                return 1;
            }
            set
            {
                if (1 != value)
                {
                    throw new NotSupportedException();
                }
            }
        }

        //[
        //Browsable(false),
        //DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden),
        //]
        //public DbCommand UpdateCommand
        //{ // V1.2.3300
        //    get
        //    {
        //        return (DbCommand)(this.UpdateCommand);
        //    }
        //    set
        //    {
        //        this.UpdateCommand = value;
        //    }
        //}

        public DbCommand UpdateCommand
        { // V1.2.3300
            get
            {
                return _updateCommand;
            }
            set
            {
                _updateCommand = value;
            }
        }

        private System.Data.MissingMappingAction UpdateMappingAction
        {
            get
            {
                if (System.Data.MissingMappingAction.Passthrough == MissingMappingAction)
                {
                    return System.Data.MissingMappingAction.Passthrough;
                }
                return System.Data.MissingMappingAction.Error;
            }
        }

        private System.Data.MissingSchemaAction UpdateSchemaAction
        {
            get
            {
                System.Data.MissingSchemaAction action = MissingSchemaAction;
                if ((System.Data.MissingSchemaAction.Add == action) || (System.Data.MissingSchemaAction.AddWithKey == action))
                {
                    return System.Data.MissingSchemaAction.Ignore;
                }
                return System.Data.MissingSchemaAction.Error;
            }
        }

        /// <summary>Called to add a single command to the batch of commands that need to be executed as a batch, when batch updates are requested.  It must return an identifier that can be used to identify the command to GetBatchedParameter later.</summary>
        protected abstract int AddToBatch(DbCommand command);

        /// <summary>Called when batch updates are requested to clear out the contents of the batch, whether or not it's been executed.</summary>
        protected abstract void ClearBatch();

        /// <summary>Called to execute the batched update command, returns the number of rows affected, just as ExecuteNonQuery would.</summary>
        protected abstract Task<int> ExecuteBatchAsync( CancellationToken cancellationToken );

        object ICloneable.Clone()
        {
#pragma warning disable 618 // ignore obsolete warning about CloneInternals
            AdaDbDataAdapter clone = (AdaDbDataAdapter)CloneInternals();
#pragma warning restore 618
            clone.CloneFrom(this);
            return clone;
        }

        private void CloneFrom(AdaDbDataAdapter from)
        {
            this.SelectCommand = CloneCommand(from.SelectCommand);
            this.InsertCommand = CloneCommand(from.InsertCommand);
            this.UpdateCommand = CloneCommand(from.UpdateCommand);
            this.DeleteCommand = CloneCommand(from.DeleteCommand);
        }

        private static DbCommand CloneCommand(DbCommand command)
        {
            if(command is ICloneable clonableCommand)
            {
                return (DbCommand)clonableCommand.Clone();
            }

            return null;
        }

        protected virtual RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            return new RowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
        }

        protected virtual RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            return new RowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            { // release mananged objects
                var pthis = this; //(IDbDataAdapter)this; // must cast to interface to obtain correct value
                pthis.SelectCommand = null;
                pthis.InsertCommand = null;
                pthis.UpdateCommand = null;
                pthis.DeleteCommand = null;
            }
            // release unmanaged objects

            base.Dispose(disposing); // notify base classes
        }

        public async Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, CancellationToken cancellationToken )
        {
            {
                DbCommand selectCmd = this.SelectCommand;
                CommandBehavior cmdBehavior = FillCommandBehavior;
                return await FillSchemaAsync(dataTable, schemaType, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false); // MDAC 67666
            }
        }

        public override async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken )
        {
            {
                DbCommand command = this.SelectCommand;
                if (DesignMode && ((null == command) || (null == command.Connection) || string.IsNullOrEmpty(command.CommandText)))
                {
                    return new DataTable[0]; // design-time support
                }
                CommandBehavior cmdBehavior = FillCommandBehavior;
                return await this.FillSchemaAsync(dataSet, schemaType, command, AdaDbDataAdapter.DefaultSourceTableName, cmdBehavior, cancellationToken ).ConfigureAwait(false);
            }
        }

        public async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, CancellationToken cancellationToken )
        {
            {
                DbCommand selectCmd = this.SelectCommand;
                CommandBehavior cmdBehavior = FillCommandBehavior;
                return await this.FillSchemaAsync(dataSet, schemaType, selectCmd, srcTable, cmdBehavior, cancellationToken ).ConfigureAwait(false);
            }
        }

        protected virtual async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, DbCommand command, string srcTable, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            {
                if (null == dataSet)
                {
                    throw new ArgumentNullException(nameof(dataSet));
                }
                if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType))
                {
                    throw ADP.InvalidSchemaType(schemaType);
                }
                if (string.IsNullOrEmpty(srcTable))
                {
                    throw ADP.FillSchemaRequiresSourceTableName("srcTable");
                }
                if (null == command)
                {
                    throw ADP.MissingSelectCommand(method: "FillSchema");
                }
                return (DataTable[]) await this.FillSchemaInternalAsync(dataSet, null, schemaType, command, srcTable, behavior, cancellationToken ).ConfigureAwait(false);
            }
        }

        protected virtual async Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            {
                if (null == dataTable)
                {
                    throw new ArgumentNullException(nameof(dataTable));
                }
                if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType))
                {
                    throw ADP.InvalidSchemaType(schemaType);
                }
                if (null == command)
                {
                    throw ADP.MissingSelectCommand("FillSchema");
                }
                string srcTableName = dataTable.TableName;
                int index = IndexOfDataSetTable(srcTableName);
                if (-1 != index)
                {
                    srcTableName = this.TableMappings[index].SourceTable;
                }

                return (DataTable) await this.FillSchemaInternalAsync( null, dataTable, schemaType, command, srcTableName, behavior | CommandBehavior.SingleResult, cancellationToken ).ConfigureAwait(false);
            }
        }

        private async Task<object> FillSchemaInternalAsync(DataSet dataset, DataTable datatable, SchemaType schemaType, DbCommand command, string srcTable, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            object dataTables = null;
            bool restoreNullConnection = (null == command.Connection);
            try
            {
                DbConnection activeConnection = AdaDbDataAdapter.GetConnection3(this, command, "FillSchema");
                ConnectionState originalState = ConnectionState.Open;

                try
                {
                    originalState = await QuietOpenAsync( activeConnection, cancellationToken ).ConfigureAwait(false);
                    using (DbDataReader dataReader = await command.ExecuteReaderAsync( behavior | CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo, cancellationToken ).ConfigureAwait(false) )
                    {
                        if (null != datatable)
                        { // delegate to next set of protected FillSchema methods
                            dataTables = await this.FillSchemaAsync(datatable, schemaType, dataReader).ConfigureAwait(false);
                        }
                        else
                        {
                            dataTables = await this.FillSchemaAsync(dataset, schemaType, srcTable, dataReader).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    QuietClose( activeConnection, originalState);
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
            return dataTables;
        }

        public override async Task<int> FillAsync( DataSet dataSet, CancellationToken cancellationToken = default )
        {
            // delegate to Fill4
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;

            return await this.FillAsync( dataSet, 0, 0, AdaDbDataAdapter.DefaultSourceTableName, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        public async Task<int> FillAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken = default )
        {
            // delegate to Fill4
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;

            return await this.FillAsync( dataSet, 0, 0, srcTable, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        public async Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, CancellationToken cancellationToken = default )
        {
            // delegate to Fill4
            DbCommand selectCmd = this.SelectCommand;
            CommandBehavior cmdBehavior = this.FillCommandBehavior;

            return await this.FillAsync( dataSet, startRecord, maxRecords, srcTable, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
        }

        protected virtual async Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default )
        {
            if (null == dataSet)
            {
                throw ADP.FillRequires("dataSet");
            }
            if (startRecord < 0)
            {
                throw ADP.InvalidStartRecord("startRecord", startRecord);
            }
            if (maxRecords < 0)
            {
                throw ADP.InvalidMaxRecords("maxRecords", maxRecords);
            }
            if (string.IsNullOrEmpty(srcTable))
            {
                throw ADP.FillRequiresSourceTableName("srcTable");
            }
            if (null == command)
            {
                throw ADP.MissingSelectCommand("Fill");
            }
            return await this.FillInternalAsync( dataSet, null, startRecord, maxRecords, srcTable, command, behavior, cancellationToken ).ConfigureAwait(false);
        }


        public async Task<int> FillAsync(DataTable dataTable, CancellationToken cancellationToken = default )
        {
            {
                // delegate to Fill8
                DataTable[] dataTables = new DataTable[1] { dataTable };
                DbCommand selectCmd = this.SelectCommand;
                CommandBehavior cmdBehavior = FillCommandBehavior;
                return await FillAsync(dataTables, 0, 0, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
            }
        }

//      public async Task<int> FillAsync(int startRecord, int maxRecords, params DataTable[] dataTables)
        public async Task<int> FillAsync(int startRecord, int maxRecords, CancellationToken cancellationToken, params DataTable[] dataTables)
        { // V1.2.3300
            {
                // delegate to Fill8
                DbCommand selectCmd = this.SelectCommand;
                CommandBehavior cmdBehavior = FillCommandBehavior;
                return await FillAsync(dataTables, startRecord, maxRecords, selectCmd, cmdBehavior, cancellationToken ).ConfigureAwait(false);
            }
        }

        protected virtual async Task<int> FillAsync(DataTable dataTable, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default )
        {
            {
                // delegate to Fill8
                DataTable[] dataTables = new DataTable[1] { dataTable };
                return await FillAsync(dataTables, 0, 0, command, behavior, cancellationToken ).ConfigureAwait(false);
            }
        }

        protected virtual async Task<int> FillAsync(DataTable[] dataTables, int startRecord, int maxRecords, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default )
        { // V1.2.3300
            {
                if ((null == dataTables) || (0 == dataTables.Length) || (null == dataTables[0]))
                {
                    throw ADP.FillRequires("dataTable");
                }
                if (startRecord < 0)
                {
                    throw ADP.InvalidStartRecord("startRecord", startRecord);
                }
                if (maxRecords < 0)
                {
                    throw ADP.InvalidMaxRecords("maxRecords", maxRecords);
                }
                if ((1 < dataTables.Length) && ((0 != startRecord) || (0 != maxRecords)))
                {
                    throw ADP.OnlyOneTableForStartRecordOrMaxRecords();
                }
                if (null == command)
                {
                    throw ADP.MissingSelectCommand("Fill");
                }
                if (1 == dataTables.Length)
                {
                    behavior |= CommandBehavior.SingleResult;
                }
                return await FillInternalAsync(null, dataTables, startRecord, maxRecords, null, command, behavior, cancellationToken ).ConfigureAwait(false);
            }
        }

        private async Task<int> FillInternalAsync( DataSet dataset, DataTable[] datatables, int startRecord, int maxRecords, string srcTable, DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            int rowsAddedToDataSet = 0;
            bool restoreNullConnection = (null == command.Connection);
            try
            {
                DbConnection activeConnection = AdaDbDataAdapter.GetConnection3(this, command, "Fill");
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

        protected virtual IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex)
        {
            // Called to retrieve a parameter from a specific bached command, the
            // first argument is the value that was returned by AddToBatch when it
            // was called for the command.

           throw new NotSupportedException();
        }

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

        [EditorBrowsableAttribute(EditorBrowsableState.Advanced)] // MDAC 69508
        override public IDataParameter[] GetFillParameters()
        {
            IDataParameter[] value = null;
            DbCommand select = this.SelectCommand;
            if (null != select)
            {
                IDataParameterCollection parameters = select.Parameters;
                if (null != parameters)
                {
                    value = new IDataParameter[parameters.Count];
                    parameters.CopyTo(value, 0);
                }
            }
            if (null == value)
            {
                value = new IDataParameter[0];
            }
            return value;
        }

        internal DataTableMapping GetTableMapping(DataTable dataTable)
        {
            DataTableMapping tableMapping = null;
            int index = IndexOfDataSetTable(dataTable.TableName);
            if (-1 != index)
            {
                tableMapping = TableMappings[index];
            }
            if (null == tableMapping)
            {
                if (System.Data.MissingMappingAction.Error == MissingMappingAction)
                {
                    throw ADP.MissingTableMappingDestination(dataTable.TableName);
                }
                tableMapping = new DataTableMapping(dataTable.TableName, dataTable.TableName);
            }
            return tableMapping;
        }

        protected virtual void InitializeBatching()
        {
            // Called when batch updates are requested to prepare for processing
            // of a batch of commands.

           throw new NotSupportedException();
        }

        protected virtual void OnRowUpdated(RowUpdatedEventArgs value)
        {
        }

        protected virtual void OnRowUpdating(RowUpdatingEventArgs value)
        {
        }

        private void ParameterInput(IDataParameterCollection parameters, StatementType typeIndex, DataRow row, DataTableMapping mappings)
        {
            MissingMappingAction missingMapping = UpdateMappingAction;
            MissingSchemaAction missingSchema = UpdateSchemaAction;

            foreach (IDataParameter parameter in parameters)
            {
                if ((null != parameter) && (0 != (ParameterDirection.Input & parameter.Direction)))
                {

                    string columnName = parameter.SourceColumn;
                    if (!string.IsNullOrEmpty(columnName))
                    {

                        DataColumn dataColumn = mappings.GetDataColumn(columnName, null, row.Table, missingMapping, missingSchema);
                        if (null != dataColumn)
                        {
                            DataRowVersion version = AdaDbDataAdapter.GetParameterSourceVersion(typeIndex, parameter);
                            parameter.Value = row[dataColumn, version];
                        }
                        else
                        {
                            parameter.Value = null;
                        }

                        DbParameter dbparameter = (parameter as DbParameter);
                        if ((null != dbparameter) && dbparameter.SourceColumnNullMapping)
                        {
                            Debug.Assert(DbType.Int32 == parameter.DbType, "unexpected DbType");
                            parameter.Value = ADP.IsNull(parameter.Value) ? ParameterValueNullValue : ParameterValueNonNullValue;
                        }
                    }
                }
            }
        }

        private void ParameterOutput(IDataParameter parameter, DataRow row, DataTableMapping mappings, MissingMappingAction missingMapping, MissingSchemaAction missingSchema)
        {
            if (0 != (ParameterDirection.Output & parameter.Direction))
            {
                object value = parameter.Value;
                if (null != value)
                {
                    // null means default, meaning we leave the current DataRow value alone
                    string columnName = parameter.SourceColumn;
                    if (!string.IsNullOrEmpty(columnName))
                    {

                        DataColumn dataColumn = mappings.GetDataColumn(columnName, null, row.Table, missingMapping, missingSchema);
                        if (null != dataColumn)
                        {
                            if (dataColumn.ReadOnly)
                            {
                                try
                                {
                                    dataColumn.ReadOnly = false;
                                    row[dataColumn] = value;
                                }
                                finally
                                {
                                    dataColumn.ReadOnly = true;
                                }
                            }
                            else
                            {
                                row[dataColumn] = value;
                            }
                        }
                    }
                }
            }
        }

        private void ParameterOutput(IDataParameterCollection parameters, DataRow row, DataTableMapping mappings)
        {
            MissingMappingAction missingMapping = this.UpdateMappingAction;
            MissingSchemaAction  missingSchema  = this.UpdateSchemaAction;

            foreach (IDataParameter parameter in parameters)
            {
                if (null != parameter)
                {
                    this.ParameterOutput(parameter, row, mappings, missingMapping, missingSchema);
                }
            }
        }

        /// <summary>Called when batch updates are requested to cleanup after a batch update has been completed.</summary>
        protected abstract void TerminateBatching();

        public override Task<int> UpdateAsync(DataSet dataSet, CancellationToken cancellationToken = default)
        {
            //if (!TableMappings.Contains(DbDataAdapter.DefaultSourceTableName)) { // MDAC 59268
            //    throw ADP.UpdateRequiresSourceTable(DbDataAdapter.DefaultSourceTableName);
            //}

            return this.UpdateAsync( dataSet, AdaDbDataAdapter.DefaultSourceTableName, cancellationToken );
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
                int index = IndexOfDataSetTable(dataTable.TableName);
                if (-1 != index)
                {
                    tableMapping = TableMappings[index];
                }

                if (null == tableMapping)
                {
                    if (MissingMappingAction.Error == MissingMappingAction)
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
            {
                if (dataSet is null) throw new ArgumentNullException(nameof(dataSet));
                if (srcTable is null) throw new ArgumentNullException(nameof(srcTable));
                if (string.IsNullOrEmpty(srcTable)) throw new ArgumentException(message: "Update: expected a non-empty SourceTable name.", paramName: nameof(srcTable));

                int rowsAffected = 0;

                System.Data.MissingMappingAction missingMapping = UpdateMappingAction;
                DataTableMapping tableMapping = GetTableMappingBySchemaAction(srcTable, srcTable, UpdateMappingAction);
                Debug.Assert(null != tableMapping, "null TableMapping when MissingMappingAction.Error");

                // the ad-hoc scenario of no dataTable just returns
                // ad-hoc scenario is defined as MissingSchemaAction.Add or MissingSchemaAction.Ignore
                System.Data.MissingSchemaAction schemaAction = UpdateSchemaAction;
                DataTable dataTable = tableMapping.GetDataTableBySchemaAction(dataSet, schemaAction);
                if (null != dataTable)
                {
                    rowsAffected = await this.UpdateFromDataTableAsync(dataTable, tableMapping, cancellationToken ).ConfigureAwait(false);
                }
                else if (!HasTableMappings() || (-1 == TableMappings.IndexOf(tableMapping)))
                {
                    //throw error since the user didn't explicitly map this tableName to Ignore.
                    throw new InvalidOperationException(string.Format("Update unable to find TableMapping['{0}'] or DataTable '{0}'.", srcTable));
                }
                return rowsAffected;
            }
        }

        protected virtual async Task<int> UpdateAsync(DataRow[] dataRows, DataTableMapping tableMapping, CancellationToken cancellationToken)
        {
            {
                Debug.Assert((null != dataRows) && (0 < dataRows.Length), "Update: bad dataRows");
                Debug.Assert(null != tableMapping, "Update: bad DataTableMapping");

                // If records were affected, increment row count by one - that is number of rows affected in dataset.
                int cumulativeDataRowsAffected = 0;

                DbConnection[] connections = new DbConnection[5]; // one for each statementtype
                ConnectionState[] connectionStates = new ConnectionState[5]; // closed by default (== 0)

                bool useSelectConnectionState = false; // MDAC 58710
                DbCommand tmpcmd = this.SelectCommand;
                if (null != tmpcmd)
                {
                    connections[0] = tmpcmd.Connection;
                    if (null != connections[0])
                    {
                        connectionStates[0] = connections[0].State;
                        useSelectConnectionState = true;
                    }
                }

                int maxBatchCommands = Math.Min(UpdateBatchSize, dataRows.Length);

                if (maxBatchCommands < 1)
                {  // batch size of zero indicates one batch, no matter how large...
                    maxBatchCommands = dataRows.Length;
                }

                BatchCommandInfo[] batchCommands = new BatchCommandInfo[maxBatchCommands];
                DataRow[] rowBatch = new DataRow[maxBatchCommands];
                int commandCount = 0;

                // the outer try/finally is for closing any connections we may have opened
                try
                {
                    try
                    {
                        if (1 != maxBatchCommands)
                        {
                            InitializeBatching();
                        }
                        StatementType statementType = StatementType.Select;
                        DbCommand dataCommand = null;

                        // for each row which is either insert, update, or delete
                        foreach (DataRow dataRow in dataRows)
                        {
                            if (null == dataRow)
                            {
                                continue; // foreach DataRow
                            }
                            bool isCommandFromRowUpdating = false;

                            // obtain the appropriate command
                            switch (dataRow.RowState)
                            {
                                case DataRowState.Detached:
                                case DataRowState.Unchanged:
                                    continue; // foreach DataRow
                                case DataRowState.Added:
                                    statementType = StatementType.Insert;
                                    dataCommand = this.InsertCommand;
                                    break;
                                case DataRowState.Deleted:
                                    statementType = StatementType.Delete;
                                    dataCommand = this.DeleteCommand;
                                    break;
                                case DataRowState.Modified:
                                    statementType = StatementType.Update;
                                    dataCommand = this.UpdateCommand;
                                    break;
                                default:
                                    Debug.Assert(false, "InvalidDataRowState");
                                    throw ADP.InvalidDataRowState(dataRow.RowState); // out of Update without completing batch
                            }

                            // setup the event to be raised
                            RowUpdatingEventArgs rowUpdatingEvent = CreateRowUpdatingEvent(dataRow, dataCommand, statementType, tableMapping);

                            // this try/catch for any exceptions during the parameter initialization
                            try
                            {
                                dataRow.RowError = null; // MDAC 67185
                                if (null != dataCommand)
                                {
                                    // prepare the parameters for the user who then can modify them during OnRowUpdating
                                    ParameterInput(dataCommand.Parameters, statementType, dataRow, tableMapping);
                                }
                            }
                            catch (Exception e)
                            {
                                // 
                                if (!ADP.IsCatchableExceptionType(e))
                                {
                                    throw;
                                }

                                rowUpdatingEvent.Errors = e;
                                rowUpdatingEvent.Status = UpdateStatus.ErrorsOccurred;
                            }

                            this.OnRowUpdating(rowUpdatingEvent); // user may throw out of Update without completing batch

                            if( rowUpdatingEvent.Command is DbCommand tmpCommand )
                            {
                                isCommandFromRowUpdating = (dataCommand != tmpCommand);
                                dataCommand = tmpCommand;
                            }
                            
                            // handle the status from RowUpdating event
                            UpdateStatus rowUpdatingStatus = rowUpdatingEvent.Status;
                            if (UpdateStatus.Continue != rowUpdatingStatus)
                            {
                                if (UpdateStatus.ErrorsOccurred == rowUpdatingStatus)
                                {
                                    UpdatingRowStatusErrors(rowUpdatingEvent, dataRow);
                                    continue; // foreach DataRow
                                }
                                else if (UpdateStatus.SkipCurrentRow == rowUpdatingStatus)
                                {
                                    if (DataRowState.Unchanged == dataRow.RowState)
                                    { // MDAC 66286
                                        cumulativeDataRowsAffected++;
                                    }
                                    continue; // foreach DataRow
                                }
                                else if (UpdateStatus.SkipAllRemainingRows == rowUpdatingStatus)
                                {
                                    if (DataRowState.Unchanged == dataRow.RowState)
                                    { // MDAC 66286
                                        cumulativeDataRowsAffected++;
                                    }
                                    break; // execute existing batch and return
                                }
                                else
                                {
                                    throw ADP.InvalidUpdateStatus(rowUpdatingStatus);  // out of Update
                                }
                            }
                            // else onward to Append/ExecuteNonQuery/ExecuteReader

                            rowUpdatingEvent = null;
                            RowUpdatedEventArgs rowUpdatedEvent = null;

                            if (1 == maxBatchCommands)
                            {
                                if (null != dataCommand)
                                {
                                    batchCommands[0].CommandIdentifier = 0;
                                    batchCommands[0].ParameterCount = dataCommand.Parameters.Count;
                                    batchCommands[0].StatementType = statementType;
                                    batchCommands[0].UpdatedRowSource = dataCommand.UpdatedRowSource;
                                }
                                batchCommands[0].Row = dataRow;
                                rowBatch[0] = dataRow; // not doing a batch update, just simplifying code...
                                commandCount = 1;
                            }
                            else
                            {
                                Exception errors = null;

                                try
                                {
                                    if (null != dataCommand)
                                    {
                                        if (0 == (UpdateRowSource.FirstReturnedRecord & dataCommand.UpdatedRowSource))
                                        {
                                            // append the command to the commandset. If an exception
                                            // occurs, then the user must append and continue

                                            batchCommands[commandCount].CommandIdentifier = AddToBatch(dataCommand);
                                            batchCommands[commandCount].ParameterCount = dataCommand.Parameters.Count;
                                            batchCommands[commandCount].Row = dataRow;
                                            batchCommands[commandCount].StatementType = statementType;
                                            batchCommands[commandCount].UpdatedRowSource = dataCommand.UpdatedRowSource;

                                            rowBatch[commandCount] = dataRow;
                                            commandCount++;

                                            if (commandCount < maxBatchCommands)
                                            {
                                                continue; // foreach DataRow
                                            }
                                            // else onward execute the batch
                                        }
                                        else
                                        {
                                            // do not allow the expectation that returned results will be used
                                            errors = new InvalidOperationException("When batching, the command's UpdatedRowSource property value of UpdateRowSource.FirstReturnedRecord or UpdateRowSource.Both is invalid.");
                                        }
                                    }
                                    else
                                    {
                                        // null Command will force RowUpdatedEvent with ErrorsOccured without completing batch
                                        errors = ADP.UpdateRequiresCommand(statementType, isCommandFromRowUpdating);
                                    }
                                }
                                catch (Exception e)
                                { // try/catch for RowUpdatedEventArgs
                                    // 
                                    if (!ADP.IsCatchableExceptionType(e))
                                    {
                                        throw;
                                    }

                                    errors = e;
                                }

                                if (null != errors)
                                {
                                    rowUpdatedEvent = CreateRowUpdatedEvent(dataRow, dataCommand, StatementType.Batch, tableMapping);
                                    rowUpdatedEvent.Errors = errors;
                                    rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;

                                    OnRowUpdated(rowUpdatedEvent); // user may throw out of Update
                                    if (errors != rowUpdatedEvent.Errors)
                                    { // user set the error msg and we will use it
                                        for (int i = 0; i < batchCommands.Length; ++i)
                                        {
                                            batchCommands[i].Errors = null;
                                        }
                                    }

                                    cumulativeDataRowsAffected += UpdatedRowStatus(rowUpdatedEvent, batchCommands, commandCount);
                                    if (UpdateStatus.SkipAllRemainingRows == rowUpdatedEvent.Status)
                                    {
                                        break;
                                    }
                                    continue; // foreach datarow
                                }
                            }

                            rowUpdatedEvent = CreateRowUpdatedEvent(dataRow, dataCommand, statementType, tableMapping);

                            // this try/catch for any exceptions during the execution, population, output parameters
                            try
                            {
                                if (1 != maxBatchCommands)
                                {
                                    DbConnection connection = AdaDbDataAdapter.GetConnection1(this);

                                    ConnectionState state = await this.UpdateConnectionOpenAsync( connection, StatementType.Batch, connections, connectionStates, useSelectConnectionState, cancellationToken ).ConfigureAwait(false);
                                    rowUpdatedEvent.AdapterInit_(rowBatch);

                                    if (ConnectionState.Open == state)
                                    {
                                        await this.UpdateBatchExecuteAsync( batchCommands, commandCount, rowUpdatedEvent, cancellationToken ).ConfigureAwait(false);
                                    }
                                    else
                                    {
                                        // null Connection will force RowUpdatedEvent with ErrorsOccured without completing batch
                                        rowUpdatedEvent.Errors = ADP.UpdateOpenConnectionRequired(StatementType.Batch, false, state);
                                        rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
                                    }
                                }
                                else if (null != dataCommand)
                                {
                                    DbConnection connection = AdaDbDataAdapter.GetConnection4(this, dataCommand, statementType, isCommandFromRowUpdating);
                                    ConnectionState state = await this.UpdateConnectionOpenAsync( connection, statementType, connections, connectionStates, useSelectConnectionState, cancellationToken ).ConfigureAwait(false);
                                    if (ConnectionState.Open == state)
                                    {
                                        await this.UpdateRowExecuteAsync( rowUpdatedEvent, dataCommand, statementType, cancellationToken ).ConfigureAwait(false);
                                        batchCommands[0].RecordsAffected = rowUpdatedEvent.RecordsAffected;
                                        batchCommands[0].Errors = null;
                                    }
                                    else
                                    {
                                        // null Connection will force RowUpdatedEvent with ErrorsOccured without completing batch
                                        rowUpdatedEvent.Errors = ADP.UpdateOpenConnectionRequired(statementType, isCommandFromRowUpdating, state);
                                        rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
                                    }
                                }
                                else
                                {
                                    // null Command will force RowUpdatedEvent with ErrorsOccured without completing batch
                                    rowUpdatedEvent.Errors = ADP.UpdateRequiresCommand(statementType, isCommandFromRowUpdating);
                                    rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
                                }
                            }
                            catch (Exception e)
                            { // try/catch for RowUpdatedEventArgs
                                // 
                                if (!ADP.IsCatchableExceptionType(e))
                                {
                                    throw;
                                }

                                rowUpdatedEvent.Errors = e;
                                rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
                            }
                            bool clearBatchOnSkipAll = (UpdateStatus.ErrorsOccurred == rowUpdatedEvent.Status);

                            {
                                Exception errors = rowUpdatedEvent.Errors;
                                OnRowUpdated(rowUpdatedEvent); // user may throw out of Update
                                // NOTE: the contents of rowBatch are now tainted...
                                if (errors != rowUpdatedEvent.Errors)
                                { // user set the error msg and we will use it
                                    for (int i = 0; i < batchCommands.Length; ++i)
                                    {
                                        batchCommands[i].Errors = null;
                                    }
                                }
                            }
                            cumulativeDataRowsAffected += UpdatedRowStatus(rowUpdatedEvent, batchCommands, commandCount);

                            if (UpdateStatus.SkipAllRemainingRows == rowUpdatedEvent.Status)
                            {
                                if (clearBatchOnSkipAll && 1 != maxBatchCommands)
                                {
                                    ClearBatch();
                                    commandCount = 0;
                                }
                                break; // from update
                            }

                            if (1 != maxBatchCommands)
                            {
                                ClearBatch();
                                commandCount = 0;
                            }
                            for (int i = 0; i < batchCommands.Length; ++i)
                            {
                                batchCommands[i] = default(BatchCommandInfo);
                            }
                            commandCount = 0;
                        } // foreach DataRow

                        // must handle the last batch
                        if (1 != maxBatchCommands && 0 < commandCount)
                        {
                            RowUpdatedEventArgs rowUpdatedEvent = CreateRowUpdatedEvent(null, dataCommand, statementType, tableMapping);

                            try
                            {
                                DbConnection connection = AdaDbDataAdapter.GetConnection1(this);

                                ConnectionState state = await this.UpdateConnectionOpenAsync( connection, StatementType.Batch, connections, connectionStates, useSelectConnectionState, cancellationToken ).ConfigureAwait(false);

                                DataRow[] finalRowBatch = rowBatch;

                                if (commandCount < rowBatch.Length)
                                {
                                    finalRowBatch = new DataRow[commandCount];
                                    Array.Copy(rowBatch, finalRowBatch, commandCount);
                                }
                                rowUpdatedEvent.AdapterInit_(finalRowBatch);

                                if (ConnectionState.Open == state)
                                {
                                    await this.UpdateBatchExecuteAsync( batchCommands, commandCount, rowUpdatedEvent, cancellationToken );
                                }
                                else
                                {
                                    // null Connection will force RowUpdatedEvent with ErrorsOccured without completing batch
                                    rowUpdatedEvent.Errors = ADP.UpdateOpenConnectionRequired(StatementType.Batch, false, state);
                                    rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
                                }
                            }
                            catch (Exception e)
                            { // try/catch for RowUpdatedEventArgs
                                // 
                                if (!ADP.IsCatchableExceptionType(e))
                                {
                                    throw;
                                }

                                rowUpdatedEvent.Errors = e;
                                rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
                            }
                            Exception errors = rowUpdatedEvent.Errors;
                            OnRowUpdated(rowUpdatedEvent); // user may throw out of Update
                            // NOTE: the contents of rowBatch are now tainted...
                            if (errors != rowUpdatedEvent.Errors)
                            { // user set the error msg and we will use it
                                for (int i = 0; i < batchCommands.Length; ++i)
                                {
                                    batchCommands[i].Errors = null;
                                }
                            }

                            cumulativeDataRowsAffected += UpdatedRowStatus(rowUpdatedEvent, batchCommands, commandCount);
                        }
                    }
                    finally
                    {
                        if (1 != maxBatchCommands)
                        {
                            TerminateBatching();
                        }
                    }
                }
                finally
                { // try/finally for connection cleanup
                    for (int i = 0; i < connections.Length; ++i)
                    {
                        QuietClose((DbConnection)connections[i], connectionStates[i]);
                    }
                }
                return cumulativeDataRowsAffected;
            }
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
                int rowAffected;
                if (GetBatchedRecordsAffected(batchCommand.CommandIdentifier, out rowAffected, out batchCommands[bc].Errors))
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
                            IDataParameter parameter = GetBatchedParameter(batchCommand.CommandIdentifier, i);
                            ParameterOutput(parameter, batchCommand.Row, rowUpdatedEvent.TableMapping, missingMapping, missingSchema);
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

        private async Task<ConnectionState> UpdateConnectionOpenAsync(DbConnection connection, StatementType statementType, DbConnection[] connections, ConnectionState[] connectionStates, bool useSelectConnectionState, CancellationToken cancellationToken )
        {
            Debug.Assert(null != connection, "unexpected null connection");
            Debug.Assert(null != connection, "unexpected null connection");
            int index = (int)statementType;
            if (connection != connections[index])
            {
                // if the user has changed the connection on the command object
                // and we had opened that connection, close that connection
                QuietClose((DbConnection)connections[index], connectionStates[index]);

                connections[index] = connection;
                connectionStates[index] = ConnectionState.Closed; // required, open may throw

                connectionStates[index] = await QuietOpenAsync( connection, cancellationToken ).ConfigureAwait(false);

                if (useSelectConnectionState && (connections[0] == connection))
                {
                    connectionStates[index] = connections[0].State;
                }
            }

            return connection.State;
        }

        private async Task<int> UpdateFromDataTableAsync(DataTable dataTable, DataTableMapping tableMapping, CancellationToken cancellationToken )
        {
            int rowsAffected = 0;
            DataRow[] dataRows = ADP.SelectAdapterRows(dataTable, false);
            if ((null != dataRows) && (0 < dataRows.Length))
            {
                rowsAffected = await this.UpdateAsync( dataRows, tableMapping, cancellationToken ).ConfigureAwait(false);
            }
            return rowsAffected;
        }

        private async Task UpdateRowExecuteAsync( RowUpdatedEventArgs rowUpdatedEvent, DbCommand dataCommand, StatementType cmdIndex, CancellationToken cancellationToken )
        {
            Debug.Assert(null != rowUpdatedEvent, "null rowUpdatedEvent");
            Debug.Assert(null != dataCommand, "null dataCommand");
            Debug.Assert(rowUpdatedEvent.Command == dataCommand, "dataCommand differs from rowUpdatedEvent");

            bool insertAcceptChanges = true;
            UpdateRowSource updatedRowSource = dataCommand.UpdatedRowSource;
           
            if ((StatementType.Delete == cmdIndex) || (0 == (UpdateRowSource.FirstReturnedRecord & updatedRowSource)))
            {
                int recordsAffected = await dataCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

                rowUpdatedEvent.AdapterInit_( recordsAffected );
            }
            else if ((StatementType.Insert == cmdIndex) || (StatementType.Update == cmdIndex))
            {
                // we only care about the first row of the first result
                using ( DbDataReader dataReader = await dataCommand.ExecuteReaderAsync( CommandBehavior.SequentialAccess, cancellationToken ).ConfigureAwait(false) )
                {
                    AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dataReader, this.ReturnProviderSpecificTypes );
                    try
                    {
                        bool getData = false;
                        do
                        {
                            // advance to the first row returning result set
                            // determined by actually having columns in the result set
                            if (0 < readerHandler.FieldCount)
                            {
                                getData = true;
                                break;
                            }
                        }
                        while ( await dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false) );

                        if (getData && (0 != dataReader.RecordsAffected))
                        {
                            AdaSchemaMapping mapping = new AdaSchemaMapping(this, null, rowUpdatedEvent.Row.Table, readerHandler, false, SchemaType.Mapped, rowUpdatedEvent.TableMapping.SourceTable, true, null, null);

                            if ((null != mapping.DataTable) && (null != mapping.DataValues))
                            {
                                if (dataReader.Read())
                                {
                                    if ((StatementType.Insert == cmdIndex) && insertAcceptChanges)
                                    { // MDAC 64199
                                        rowUpdatedEvent.Row.AcceptChanges();
                                        insertAcceptChanges = false;
                                    }
                                    mapping.ApplyToDataRow(rowUpdatedEvent.Row);
                                }
                            }
                        }
                    }
                    finally
                    {
                        // using Close which can optimize its { while(dataReader.NextResult()); } loop
                        dataReader.Close();

                        // RecordsAffected is available after Close, but don't trust it after Dispose
                        int recordsAffected = dataReader.RecordsAffected;
                        rowUpdatedEvent.AdapterInit_(recordsAffected);
                    }
                }
            }
            else
            {
                // StatementType.Select, StatementType.Batch
                Debug.Assert(false, "unexpected StatementType");
            }

            // map the parameter results to the dataSet
            if
                (
                    (
                        (StatementType.Insert == cmdIndex)
                        ||
                        (StatementType.Update == cmdIndex)
                    )
                    &&
                    (
                        0 != (UpdateRowSource.OutputParameters & updatedRowSource)
                    )
                    &&
                    (0 != rowUpdatedEvent.RecordsAffected)
                )
            {

                if ((StatementType.Insert == cmdIndex) && insertAcceptChanges)
                {
                    rowUpdatedEvent.Row.AcceptChanges();
                }
                
                this.ParameterOutput(dataCommand.Parameters, rowUpdatedEvent.Row, rowUpdatedEvent.TableMapping);
            }

            // Only error if RecordsAffect == 0, not -1.  A value of -1 means no count was received from server,
            // do not error in that situation (means 'set nocount on' was executed on server).
            switch (rowUpdatedEvent.Status)
            {
                case UpdateStatus.Continue:
                    switch (cmdIndex)
                    {
                        case StatementType.Update:
                        case StatementType.Delete:
                            if (0 == rowUpdatedEvent.RecordsAffected)
                            {
                                // bug50526, an exception if no records affected and attempted an Update/Delete
                                Debug.Assert(null == rowUpdatedEvent.Errors, "Continue - but contains an exception");
                                rowUpdatedEvent.Errors = ADP.UpdateConcurrencyViolation(cmdIndex, rowUpdatedEvent.RecordsAffected, 1, new DataRow[] { rowUpdatedEvent.Row }); // MDAC 55735
                                rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
                            }
                            break;
                    }
                    break;
            }
        }

        private int UpdatedRowStatus(RowUpdatedEventArgs rowUpdatedEvent, BatchCommandInfo[] batchCommands, int commandCount)
        {
            Debug.Assert(null != rowUpdatedEvent, "null rowUpdatedEvent");
            int cumulativeDataRowsAffected = 0;
            switch (rowUpdatedEvent.Status)
            {
                case UpdateStatus.Continue:
                    cumulativeDataRowsAffected = UpdatedRowStatusContinue(rowUpdatedEvent, batchCommands, commandCount);
                    break; // return to foreach DataRow
                case UpdateStatus.ErrorsOccurred:
                    cumulativeDataRowsAffected = UpdatedRowStatusErrors(rowUpdatedEvent, batchCommands, commandCount);
                    break; // no datarow affected if ErrorsOccured
                case UpdateStatus.SkipCurrentRow:
                case UpdateStatus.SkipAllRemainingRows: // cancel the Update method
                    cumulativeDataRowsAffected = UpdatedRowStatusSkip(batchCommands, commandCount);
                    break; // foreach DataRow without accepting changes on this row (but user may haved accepted chagnes for us)
                default:
                    throw ADP.InvalidUpdateStatus(rowUpdatedEvent.Status);
            } // switch RowUpdatedEventArgs.Status
            return cumulativeDataRowsAffected;
        }

        private int UpdatedRowStatusContinue(RowUpdatedEventArgs rowUpdatedEvent, BatchCommandInfo[] batchCommands, int commandCount)
        {
            Debug.Assert(null != batchCommands, "null batchCommands?");
            int cumulativeDataRowsAffected = 0;
            // 1. We delay accepting the changes until after we fire RowUpdatedEvent
            //    so the user has a chance to call RejectChanges for any given reason
            // 2. If the DataSource return 0 records affected, its an indication that
            //    the command didn't take so we don't want to automatically
            //    AcceptChanges.
            // With 'set nocount on' the count will be -1, accept changes in that case too.
            // 3.  Don't accept changes if no rows were affected, the user needs
            //     to know that there is a concurrency violation

            // Only accept changes if the row is not already accepted, ie detached.
            bool acdu = AcceptChangesDuringUpdate;
            for (int i = 0; i < commandCount; i++)
            {
                DataRow row = batchCommands[i].Row;
                if ((null == batchCommands[i].Errors) && batchCommands[i].RecordsAffected.HasValue && (0 != batchCommands[i].RecordsAffected.Value))
                {
                    Debug.Assert(null != row, "null dataRow?");
                    if (acdu)
                    {
                        if (0 != ((DataRowState.Added | DataRowState.Deleted | DataRowState.Modified) & row.RowState))
                        {
                            row.AcceptChanges();
                        }
                    }
                    cumulativeDataRowsAffected++;
                }
            }
            return cumulativeDataRowsAffected;
        }

        private int UpdatedRowStatusErrors(RowUpdatedEventArgs rowUpdatedEvent, BatchCommandInfo[] batchCommands, int commandCount)
        {
            Debug.Assert(null != batchCommands, "null batchCommands?");
            Exception errors = rowUpdatedEvent.Errors;
            if (null == errors)
            {
                // user changed status to ErrorsOccured without supplying an exception message
                errors = new DataException("RowUpdatedEvent: Errors occurred; no additional is information available.");
                rowUpdatedEvent.Errors = errors;
            }

            int affected = 0;
            bool done = false;
            string message = errors.Message;

            for (int i = 0; i < commandCount; i++)
            {
                DataRow row = batchCommands[i].Row;
                Debug.Assert(null != row, "null dataRow?");

                if (null != batchCommands[i].Errors)
                { // will exist if 0 == RecordsAffected
                    string rowMsg = batchCommands[i].Errors.Message;
                    if (String.IsNullOrEmpty(rowMsg))
                    {
                        rowMsg = message;
                    }
                    row.RowError += rowMsg;
                    done = true;
                }
            }
            if (!done)
            { // all rows are in 'error'
                for (int i = 0; i < commandCount; i++)
                {
                    DataRow row = batchCommands[i].Row;
                    // its possible a DBConcurrencyException exists and all rows have records affected
                    // via not overriding GetBatchedRecordsAffected or user setting the exception
                    row.RowError += message; // MDAC 65808
                }
            }
            else
            {
                affected = UpdatedRowStatusContinue(rowUpdatedEvent, batchCommands, commandCount);
            }
            if (!ContinueUpdateOnError)
            { // MDAC 66900
                throw errors; // out of Update
            }
            return affected; // return the count of successful rows within the batch failure
        }

        private int UpdatedRowStatusSkip(BatchCommandInfo[] batchCommands, int commandCount)
        {
            Debug.Assert(null != batchCommands, "null batchCommands?");

            int cumulativeDataRowsAffected = 0;

            for (int i = 0; i < commandCount; i++)
            {
                DataRow row = batchCommands[i].Row;
                Debug.Assert(null != row, "null dataRow?");
                if (0 != ((DataRowState.Detached | DataRowState.Unchanged) & row.RowState))
                {
                    cumulativeDataRowsAffected++; // MDAC 66286
                }
            }
            return cumulativeDataRowsAffected;
        }

        private void UpdatingRowStatusErrors(RowUpdatingEventArgs rowUpdatedEvent, DataRow dataRow)
        {
            Debug.Assert(null != dataRow, "null dataRow");
            Exception errors = rowUpdatedEvent.Errors;

            if (null == errors)
            {
                // user changed status to ErrorsOccured without supplying an exception message
                errors = new DataException("RowUpdatingEvent: Errors occurred; no additional is information available.");
                rowUpdatedEvent.Errors = errors;
            }
            string message = errors.Message;
            dataRow.RowError += message; // MDAC 65808

            if (!ContinueUpdateOnError)
            { // MDAC 66900
                throw errors; // out of Update
            }
        }

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

        private static DbConnection GetConnection3(AdaDbDataAdapter adapter, DbCommand command, string method)
        {
            Debug.Assert(null != command, "GetConnection3: null command");
            Debug.Assert(!string.IsNullOrEmpty(method), "missing method name");
            DbConnection connection = command.Connection;
            if (null == connection)
            {
                throw ADP.ConnectionRequired_Res(method);
            }
            return connection;
        }

        private static DbConnection GetConnection4(AdaDbDataAdapter adapter, DbCommand command, StatementType statementType, bool isCommandFromRowUpdating)
        {
            Debug.Assert(null != command, "GetConnection4: null command");
            DbConnection connection = command.Connection;
            if (null == connection)
            {
                throw ADP.UpdateConnectionRequired(statementType, isCommandFromRowUpdating);
            }
            return connection;
        }

        #endregion

        private static DataRowVersion GetParameterSourceVersion(StatementType statementType, IDataParameter parameter)
        {
            switch (statementType)
            {
                case StatementType.Insert: return DataRowVersion.Current;  // ignores parameter.SourceVersion
                case StatementType.Update: return parameter.SourceVersion;
                case StatementType.Delete: return DataRowVersion.Original; // ignores parameter.SourceVersion
                case StatementType.Select:
                case StatementType.Batch:
                    throw new ArgumentException(message: string.Format("Unwanted statement type {0}", statementType.ToString()), paramName: nameof(statementType));
                default:
                    throw ADP.InvalidStatementType(statementType);
            }
        }

        /// <summary></summary>
        /// <remarks><see cref="QuietOpenAsync"/> needs to appear in the try {} finally { QuietClose } block otherwise a possibility exists that an exception may be thrown, i.e. <see cref="ThreadAbortException"/> where we would Open the connection and not close it</remarks>
        /// <param name="connection"></param>
        /// <returns></returns>
        private static async Task<ConnectionState> QuietOpenAsync(DbConnection connection, CancellationToken cancellationToken )
        {
            Debug.Assert(null != connection, "QuietOpen: null connection");
            var originalState = connection.State;
            if (ConnectionState.Closed == originalState)
            {
                await connection.OpenAsync( cancellationToken ).ConfigureAwait(false);
            }

            return originalState;
        }

        private static void QuietClose(DbConnection connection, ConnectionState originalState)
        {
            // close the connection if:
            // * it was closed on first use and adapter has opened it, AND
            // * provider's implementation did not ask to keep this connection open
            if ((null != connection) && (ConnectionState.Closed == originalState))
            {
                // we don't have to check the current connection state because
                // it is supposed to be safe to call Close multiple times
                connection.Close();
            }
        }
    }
}
