using System;
using System.ComponentModel;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    // TODO: Many of these methods aren't async and so could use reflection instead of being reimplemented.

    public static partial class AsyncDataReaderBatchExecuteMethods
    {
        public static async Task UpdateBatchExecuteAsync( IBatchingAdapter adapter, BatchCommandInfo[] batchCommands, int commandCount, RowUpdatedEventArgs rowUpdatedEvent, CancellationToken cancellationToken )
        {
            try
            {
                // the batch execution may succeed, partially succeed and throw an exception (or not), or totally fail
                int recordsAffected = await adapter.ExecuteBatchAsync( cancellationToken ).ConfigureAwait(false);
                rowUpdatedEvent.AdapterInit_(recordsAffected);
            }
            catch (DbException e)
            {
                // an exception was thrown be but some part of the batch may have been succesfull
                rowUpdatedEvent.Errors = e;
                rowUpdatedEvent.Status = UpdateStatus.ErrorsOccurred;
            }

            AfterUpdateBatchExecute( adapter, batchCommands, commandCount, rowUpdatedEvent );
        }

        public static void AfterUpdateBatchExecute( IBatchingAdapter adapter, BatchCommandInfo[] batchCommands, int commandCount, RowUpdatedEventArgs rowUpdatedEvent )
        {
            MissingMappingAction missingMapping = adapter.UpdateMappingAction;
            MissingSchemaAction  missingSchema  = adapter.UpdateSchemaAction;

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
                if (adapter.GetBatchedRecordsAffected(batchCommand.CommandIdentifier, out int rowAffected, error: out batchCommands[bc].Errors))
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
                            IDataParameter parameter = adapter.GetBatchedParameter(batchCommand.CommandIdentifier, i);
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

        
    }
}
