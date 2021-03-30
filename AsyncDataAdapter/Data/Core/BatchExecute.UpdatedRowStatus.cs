using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace AsyncDataAdapter.Internal
{
    // TODO: Many of these methods aren't async and so could use reflection instead of being reimplemented.

    public static partial class AsyncDataReaderBatchExecuteMethods
    {
        public static int UpdatedRowStatus( IUpdatedRowOptions opts, RowUpdatedEventArgs rowUpdatedEvent, BatchCommandInfo[] batchCommands, int commandCount )
        {
            Debug.Assert(null != rowUpdatedEvent, "null rowUpdatedEvent");
            int cumulativeDataRowsAffected;
            switch (rowUpdatedEvent.Status)
            {
                case UpdateStatus.Continue:
                    cumulativeDataRowsAffected = UpdatedRowStatusContinue( opts, batchCommands, commandCount );
                    break; // return to foreach DataRow
                
                case UpdateStatus.ErrorsOccurred:
                    cumulativeDataRowsAffected = UpdatedRowStatusErrors( opts, rowUpdatedEvent, batchCommands, commandCount );
                    break; // no datarow affected if ErrorsOccured
                
                case UpdateStatus.SkipCurrentRow:
                case UpdateStatus.SkipAllRemainingRows: // cancel the Update method
                    cumulativeDataRowsAffected = UpdatedRowStatusSkip( batchCommands, commandCount );
                    break; // foreach DataRow without accepting changes on this row (but user may haved accepted chagnes for us)
                
                default:
                    throw ADP.InvalidUpdateStatus( rowUpdatedEvent.Status );
            }

            return cumulativeDataRowsAffected;
        }

        public static int UpdatedRowStatusContinue( IUpdatedRowOptions opts, BatchCommandInfo[] batchCommands, int commandCount)
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
            bool acdu = opts.AcceptChangesDuringUpdate;
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

        public static int UpdatedRowStatusErrors( IUpdatedRowOptions opts, RowUpdatedEventArgs rowUpdatedEvent, BatchCommandInfo[] batchCommands, int commandCount)
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
                affected = UpdatedRowStatusContinue( opts, batchCommands, commandCount );
            }

            if (!opts.ContinueUpdateOnError)
            {
                throw errors; // out of Update
            }
            return affected; // return the count of successful rows within the batch failure
        }

        public static int UpdatedRowStatusSkip(BatchCommandInfo[] batchCommands, int commandCount)
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
    }
}
