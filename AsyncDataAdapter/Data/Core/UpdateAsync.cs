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
        public static Task<int> UpdateAsync<TAdapter>( TAdapter self, DataTable dataTable, CancellationToken cancellationToken )
            where TAdapter : ICanUpdateAsync, IAdaSchemaMappingAdapter
        {
            if (dataTable is null) throw new ArgumentNullException(nameof(dataTable));

            DataTableMapping tableMapping = null;

            int index = self.IndexOfDataSetTable(dataTable.TableName);
            if (-1 != index)
            {
                tableMapping = self.TableMappings[index];
            }

            if (null == tableMapping)
            {
                if (MissingMappingAction.Error == self.MissingMappingAction)
                {
                    throw ADP.MissingTableMappingDestination(dataTable.TableName);
                }

                tableMapping = new DataTableMapping( DbDataAdapter.DefaultSourceTableName, dataTable.TableName );
            }

            return UpdateFromDataTableAsync( self, dataTable, tableMapping, cancellationToken );
        }

        public static async Task<int> UpdateAsync<TAdapter>( TAdapter self, DataSet dataSet, string srcTable, CancellationToken cancellationToken )
            where TAdapter : ICanUpdateAsync, IAdaSchemaMappingAdapter
        {
            if (dataSet is null) throw new ArgumentNullException(nameof(dataSet));
            if (srcTable is null) throw new ArgumentNullException(nameof(srcTable));
            if (string.IsNullOrEmpty(srcTable)) throw new ArgumentException(message: "Update: expected a non-empty SourceTable name.", paramName: nameof(srcTable));

            int rowsAffected = 0;

            DataTableMapping tableMapping = self.GetTableMappingBySchemaAction( sourceTableName: srcTable, dataSetTableName: srcTable, mappingAction: self.UpdateMappingAction );
            
            Debug.Assert(null != tableMapping, "null TableMapping when MissingMappingAction.Error");

            // the ad-hoc scenario of no dataTable just returns
            // ad-hoc scenario is defined as MissingSchemaAction.Add or MissingSchemaAction.Ignore
            MissingSchemaAction schemaAction = self.UpdateSchemaAction;
            DataTable dataTable = tableMapping.GetDataTableBySchemaAction(dataSet, schemaAction);
            if (null != dataTable)
            {
                rowsAffected = await UpdateFromDataTableAsync( self, dataTable, tableMapping, cancellationToken ).ConfigureAwait(false);
            }
            else if ( (self.TableMappings?.Count ?? 0) == 0 || (-1 == self.TableMappings.IndexOf(tableMapping)))
            {
                //throw error since the user didn't explicitly map this tableName to Ignore.
                throw new InvalidOperationException(string.Format("Update unable to find TableMapping['{0}'] or DataTable '{0}'.", srcTable));
            }

            return rowsAffected;
        }

        public static async Task<ConnectionState> UpdateConnectionOpenAsync( DbConnection connection, StatementType statementType, DbConnection[] connections, ConnectionState[] connectionStates, bool useSelectConnectionState, CancellationToken cancellationToken )
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

        public static async Task<int> UpdateFromDataTableAsync( ICanUpdateAsync self, DataTable dataTable, DataTableMapping tableMapping, CancellationToken cancellationToken )
        {
            int rowsAffected = 0;
            DataRow[] dataRows = Utility.SelectAdapterRows(dataTable, false);
            if ((null != dataRows) && (0 < dataRows.Length))
            {
                rowsAffected = await UpdateAsync( self, dataRows, tableMapping, cancellationToken ).ConfigureAwait(false);
            }
            return rowsAffected;
        }

        public static async Task UpdateRowExecuteAsync( IAdaSchemaMappingAdapter adapter, Boolean returnProviderSpecificTypes, RowUpdatedEventArgs rowUpdatedEvent, DbCommand dataCommand, StatementType cmdIndex, CancellationToken cancellationToken )
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
                    AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dataReader, returnProviderSpecificTypes );
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
                            AdaSchemaMapping mapping = new AdaSchemaMapping( adapter, null, rowUpdatedEvent.Row.Table, readerHandler, false, SchemaType.Mapped, rowUpdatedEvent.TableMapping.SourceTable, true, null, null);

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
                
                ParameterMethods.ParameterOutput( adapter.MissingMappingAction, adapter.MissingSchemaAction, dataCommand.Parameters, rowUpdatedEvent.Row, rowUpdatedEvent.TableMapping);
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

        public static void UpdatingRowStatusErrors( Boolean continueUpdateOnError, RowUpdatingEventArgs rowUpdatedEvent, DataRow dataRow )
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
            dataRow.RowError += message;

            if (!continueUpdateOnError)
            {
                throw errors; // out of Update
            }
        }
    }
}
