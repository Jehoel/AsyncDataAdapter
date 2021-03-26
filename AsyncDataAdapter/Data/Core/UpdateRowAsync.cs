using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    using static AsyncDataReaderConnectionMethods;

    public static partial class AsyncDataReaderUpdateMethods
    {
        public static async Task<int> UpdateAsync( ICanUpdateAsync self, DataRow[] dataRows, DataTableMapping tableMapping, CancellationToken cancellationToken )
        {
            Debug.Assert((null != dataRows) && (0 < dataRows.Length), "Update: bad dataRows");
            Debug.Assert(null != tableMapping, "Update: bad DataTableMapping");

            // If records were affected, increment row count by one - that is number of rows affected in dataset.
            int cumulativeDataRowsAffected = 0;

            DbConnection[] connections = new DbConnection[5]; // one for each statementtype
            ConnectionState[] connectionStates = new ConnectionState[5]; // closed by default (== 0)

            bool useSelectConnectionState = false; // MDAC 58710
            DbCommand tmpcmd = self.SelectCommand;
            if (null != tmpcmd)
            {
                connections[0] = tmpcmd.Connection;
                if (null != connections[0])
                {
                    connectionStates[0] = connections[0].State;
                    useSelectConnectionState = true;
                }
            }

            int maxBatchCommands = Math.Min(self.UpdateBatchSize, dataRows.Length);

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
                        self.InitializeBatching();
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
                                dataCommand = self.InsertCommand;
                                break;
                            case DataRowState.Deleted:
                                statementType = StatementType.Delete;
                                dataCommand = self.DeleteCommand;
                                break;
                            case DataRowState.Modified:
                                statementType = StatementType.Update;
                                dataCommand = self.UpdateCommand;
                                break;
                            default:
                                Debug.Assert(false, "InvalidDataRowState");
                                throw ADP.InvalidDataRowState(dataRow.RowState); // out of Update without completing batch
                        }

                        // setup the event to be raised
                        RowUpdatingEventArgs rowUpdatingEvent = self.CreateRowUpdatingEvent(dataRow, dataCommand, statementType, tableMapping);

                        // self try/catch for any exceptions during the parameter initialization
                        try
                        {
                            dataRow.RowError = null; // MDAC 67185
                            if (null != dataCommand)
                            {
                                // prepare the parameters for the user who then can modify them during OnRowUpdating
                                ParameterMethods.ParameterInput( self.UpdateMappingAction, self.UpdateSchemaAction, dataCommand.Parameters, statementType, dataRow, tableMapping);
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

                        self.OnRowUpdating(rowUpdatingEvent); // user may throw out of Update without completing batch

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
                                self.UpdatingRowStatusErrors(rowUpdatingEvent, dataRow);
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

                                        batchCommands[commandCount].CommandIdentifier = self.AddToBatch(dataCommand);
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
                                rowUpdatedEvent = self.CreateRowUpdatedEvent(dataRow, dataCommand, StatementType.Batch, tableMapping);
                                rowUpdatedEvent.Errors = errors;
                                rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;

                                self.OnRowUpdated(rowUpdatedEvent); // user may throw out of Update
                                if (errors != rowUpdatedEvent.Errors)
                                { // user set the error msg and we will use it
                                    for (int i = 0; i < batchCommands.Length; ++i)
                                    {
                                        batchCommands[i].Errors = null;
                                    }
                                }

                                cumulativeDataRowsAffected += self.UpdatedRowStatus(rowUpdatedEvent, batchCommands, commandCount);
                                if (UpdateStatus.SkipAllRemainingRows == rowUpdatedEvent.Status)
                                {
                                    break;
                                }
                                continue; // foreach datarow
                            }
                        }

                        rowUpdatedEvent = self.CreateRowUpdatedEvent(dataRow, dataCommand, statementType, tableMapping);

                        // self try/catch for any exceptions during the execution, population, output parameters
                        try
                        {
                            if (1 != maxBatchCommands)
                            {
                                DbConnection connection = self.GetConnection();

                                ConnectionState state = await self.UpdateConnectionOpenAsync( connection, StatementType.Batch, connections, connectionStates, useSelectConnectionState, cancellationToken ).ConfigureAwait(false);
                                rowUpdatedEvent.AdapterInit_(rowBatch);

                                if (ConnectionState.Open == state)
                                {
                                    await AsyncDataReaderBatchExecuteMethods.UpdateBatchExecuteAsync( self, batchCommands, commandCount, rowUpdatedEvent, cancellationToken ).ConfigureAwait(false);
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
                                DbConnection connection = dataCommand.Connection ?? throw new InvalidOperationException( "DbCommand.Connection is null." );
                                ConnectionState state = await self.UpdateConnectionOpenAsync( connection, statementType, connections, connectionStates, useSelectConnectionState, cancellationToken ).ConfigureAwait(false);
                                if (ConnectionState.Open == state)
                                {
                                    await self.UpdateRowExecuteAsync( rowUpdatedEvent, dataCommand, statementType, cancellationToken ).ConfigureAwait(false);
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
                            self.OnRowUpdated(rowUpdatedEvent); // user may throw out of Update
                            // NOTE: the contents of rowBatch are now tainted...
                            if (errors != rowUpdatedEvent.Errors)
                            { // user set the error msg and we will use it
                                for (int i = 0; i < batchCommands.Length; ++i)
                                {
                                    batchCommands[i].Errors = null;
                                }
                            }
                        }
                        cumulativeDataRowsAffected += self.UpdatedRowStatus(rowUpdatedEvent, batchCommands, commandCount);

                        if (UpdateStatus.SkipAllRemainingRows == rowUpdatedEvent.Status)
                        {
                            if (clearBatchOnSkipAll && 1 != maxBatchCommands)
                            {
                                self.ClearBatch();
                                commandCount = 0;
                            }
                            break; // from update
                        }

                        if (1 != maxBatchCommands)
                        {
                            self.ClearBatch();
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
                        RowUpdatedEventArgs rowUpdatedEvent = self.CreateRowUpdatedEvent(null, dataCommand, statementType, tableMapping);

                        try
                        {
                            DbConnection connection = self.GetConnection();

                            ConnectionState state = await self.UpdateConnectionOpenAsync( connection, StatementType.Batch, connections, connectionStates, useSelectConnectionState, cancellationToken ).ConfigureAwait(false);

                            DataRow[] finalRowBatch = rowBatch;

                            if (commandCount < rowBatch.Length)
                            {
                                finalRowBatch = new DataRow[commandCount];
                                Array.Copy(rowBatch, finalRowBatch, commandCount);
                            }
                            rowUpdatedEvent.AdapterInit_(finalRowBatch);

                            if (ConnectionState.Open == state)
                            {
                                await AsyncDataReaderBatchExecuteMethods.UpdateBatchExecuteAsync( self, batchCommands, commandCount, rowUpdatedEvent, cancellationToken );
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
                        self.OnRowUpdated(rowUpdatedEvent); // user may throw out of Update
                        // NOTE: the contents of rowBatch are now tainted...
                        if (errors != rowUpdatedEvent.Errors)
                        { // user set the error msg and we will use it
                            for (int i = 0; i < batchCommands.Length; ++i)
                            {
                                batchCommands[i].Errors = null;
                            }
                        }

                        cumulativeDataRowsAffected += self.UpdatedRowStatus(rowUpdatedEvent, batchCommands, commandCount);
                    }
                }
                finally
                {
                    if (1 != maxBatchCommands)
                    {
                        self.TerminateBatching();
                    }
                }
            }
            finally
            {
                // try/finally for connection cleanup
                for (int i = 0; i < connections.Length; ++i)
                {
                    QuietClose( connections[i], connectionStates[i] );
                }
            }

            return cumulativeDataRowsAffected;
        }
    }
}
