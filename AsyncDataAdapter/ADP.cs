//------------------------------------------------------------------------------
// <copyright file="AdapterUtil.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
// <owner current="true" primary="true">[....]</owner>
// <owner current="true" primary="false">[....]</owner>
//------------------------------------------------------------------------------


using System.Data.Common;

namespace AsyncDataAdapter
{

    using Microsoft.Win32;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Configuration;
    using System.Data;
    using System.Data.SqlTypes;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using System.Runtime.ConstrainedExecution;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;
    using System.Security;
    using System.Security.Permissions;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml;
    // using SysTx = System.Transactions;
    // using SysES = System.EnterpriseServices;
    using System.Runtime.Versioning;

    internal static class ADP
    {
        //
        // COM+ exceptions
        //
        static internal ArgumentException Argument(string error)
        {
            ArgumentException e = new ArgumentException(error);
            return e;
        }
        static internal ArgumentException Argument(string error, Exception inner)
        {
            ArgumentException e = new ArgumentException(error, inner);
            return e;
        }
        static internal ArgumentException Argument(string error, string parameter)
        {
            ArgumentException e = new ArgumentException(error, parameter);
            return e;
        }
        static internal ArgumentException Argument(string error, string parameter, Exception inner)
        {
            ArgumentException e = new ArgumentException(error, parameter, inner);
            return e;
        }
        static internal ArgumentNullException ArgumentNull(string parameter)
        {
            ArgumentNullException e = new ArgumentNullException(parameter);
            return e;
        }
        static internal ArgumentNullException ArgumentNull(string parameter, string error)
        {
            ArgumentNullException e = new ArgumentNullException(parameter, error);
            return e;
        }
        static internal ArgumentOutOfRangeException ArgumentOutOfRange(string parameterName)
        {
            ArgumentOutOfRangeException e = new ArgumentOutOfRangeException(parameterName);
            return e;
        }
        static internal ArgumentOutOfRangeException ArgumentOutOfRange(string message, string parameterName)
        {
            ArgumentOutOfRangeException e = new ArgumentOutOfRangeException(parameterName, message);
            return e;
        }
        static internal ArgumentOutOfRangeException ArgumentOutOfRange(string message, string parameterName, object value)
        {
            ArgumentOutOfRangeException e = new ArgumentOutOfRangeException(parameterName, value, message);
            return e;
        }
        static internal DataException Data(string message)
        {
            DataException e = new DataException(message);
            return e;
        }
        static internal IndexOutOfRangeException IndexOutOfRange(int value)
        {
            IndexOutOfRangeException e = new IndexOutOfRangeException(value.ToString(CultureInfo.InvariantCulture));
            return e;
        }
        static internal IndexOutOfRangeException IndexOutOfRange(string error)
        {
            IndexOutOfRangeException e = new IndexOutOfRangeException(error);
            return e;
        }
        static internal IndexOutOfRangeException IndexOutOfRange()
        {
            IndexOutOfRangeException e = new IndexOutOfRangeException();
            return e;
        }
        static internal InvalidCastException InvalidCast(string error)
        {
            return InvalidCast(error, null);
        }
        static internal InvalidCastException InvalidCast(string error, Exception inner)
        {
            InvalidCastException e = new InvalidCastException(error, inner);
            return e;
        }
        static internal InvalidOperationException InvalidOperation(string error)
        {
            InvalidOperationException e = new InvalidOperationException(error);
            return e;
        }
        static internal InvalidOperationException InvalidOperation(string error, Exception inner)
        {
            InvalidOperationException e = new InvalidOperationException(error, inner);
            return e;
        }
        static internal NotSupportedException NotSupported()
        {
            NotSupportedException e = new NotSupportedException();
            return e;
        }
        static internal InvalidCastException InvalidCast()
        {
            InvalidCastException e = new InvalidCastException();
            return e;
        }
        static internal IOException IO(string error)
        {
            IOException e = new IOException(error);
            return e;
        }
        static internal IOException IO(string error, Exception inner)
        {
            IOException e = new IOException(error, inner);
            return e;
        }
        static internal InvalidOperationException DataAdapter(string error)
        {
            return InvalidOperation(error);
        }
        static internal InvalidOperationException DataAdapter(string error, Exception inner)
        {
            return InvalidOperation(error, inner);
        }
        static private InvalidOperationException Provider(string error)
        {
            return InvalidOperation(error);
        }
        static internal ObjectDisposedException ObjectDisposed(object instance)
        {
            ObjectDisposedException e = new ObjectDisposedException(instance.GetType().Name);
            return e;
        }

        static internal void CheckArgumentLength(Array value, string parameterName)
        {
            CheckArgumentNull(value, parameterName);
            if (0 == value.Length)
            {
                // throw Argument(Res.GetString(Res.ADP_EmptyArray, parameterName));
                throw Argument(string.Format("Argument is empty: {0}", parameterName));
            }
        }
        static internal void CheckArgumentNull(object value, string parameterName)
        {
            if (null == value)
            {
                throw ArgumentNull(parameterName);
            }
        }


        // only StackOverflowException & ThreadAbortException are sealed classes
        static private readonly Type StackOverflowType = typeof(StackOverflowException);
        static private readonly Type OutOfMemoryType = typeof(OutOfMemoryException);
        static private readonly Type ThreadAbortType = typeof(ThreadAbortException);
        static private readonly Type NullReferenceType = typeof(NullReferenceException);
        static private readonly Type AccessViolationType = typeof(AccessViolationException);
        static private readonly Type SecurityType = typeof(SecurityException);

        static internal bool IsCatchableExceptionType(Exception e)
        {
            // a 'catchable' exception is defined by what it is not.
            Debug.Assert(e != null, "Unexpected null exception!");
            Type type = e.GetType();

            return ((type != StackOverflowType) &&
                     (type != OutOfMemoryType) &&
                     (type != ThreadAbortType) &&
                     (type != NullReferenceType) &&
                     (type != AccessViolationType) &&
                     !SecurityType.IsAssignableFrom(type));
        }

        static internal bool IsCatchableOrSecurityExceptionType(Exception e)
        {
            // a 'catchable' exception is defined by what it is not.
            // since IsCatchableExceptionType defined SecurityException as not 'catchable'
            // this method will return true for SecurityException has being catchable.

            // the other way to write this method is, but then SecurityException is checked twice
            // return ((e is SecurityException) || IsCatchableExceptionType(e));

            Debug.Assert(e != null, "Unexpected null exception!");
            Type type = e.GetType();

            return ((type != StackOverflowType) &&
                     (type != OutOfMemoryType) &&
                     (type != ThreadAbortType) &&
                     (type != NullReferenceType) &&
                     (type != AccessViolationType));
        }

        // Invalid Enumeration

        static internal ArgumentOutOfRangeException InvalidEnumerationValue(Type type, int value)
        {
            // return ADP.ArgumentOutOfRange(Res.GetString(Res.ADP_InvalidEnumerationValue, type.Name, value.ToString(System.Globalization.CultureInfo.InvariantCulture)), type.Name);
            return ADP.ArgumentOutOfRange(string.Format("Argument {0} is out of range {1}", type.Name, value.ToString(System.Globalization.CultureInfo.InvariantCulture)), type.Name);
        }

        static internal ArgumentOutOfRangeException NotSupportedEnumerationValue(Type type, string value, string method)
        {
            // return ADP.ArgumentOutOfRange(Res.GetString(Res.ADP_NotSupportedEnumerationValue, type.Name, value, method), type.Name);
            return ADP.ArgumentOutOfRange(string.Format("{0} value {1} not supported in {2}", type.Name, value, method), type.Name);
        }

        // DbCommandBuilder.CatalogLocation
        static internal ArgumentOutOfRangeException InvalidCatalogLocation(CatalogLocation value)
        {
#if DEBUG
            switch (value)
            {
                case CatalogLocation.Start:
                case CatalogLocation.End:
                    Debug.Assert(false, "valid CatalogLocation " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(CatalogLocation), (int)value);
        }

        static internal ArgumentOutOfRangeException InvalidCommandBehavior(CommandBehavior value)
        {
#if DEBUG
            if ((0 <= (int)value) && ((int)value <= 0x3F))
            {
                Debug.Assert(false, "valid CommandType " + value.ToString());
            }
#endif
            return InvalidEnumerationValue(typeof(CommandBehavior), (int)value);
        }
        static internal void ValidateCommandBehavior(CommandBehavior value)
        {
            if (((int)value < 0) || (0x3F < (int)value))
            {
                throw InvalidCommandBehavior(value);
            }
        }

        //static internal ArgumentException MustBeReadOnly(string argumentName)
        //{
        //    return Argument(Res.GetString(Res.ADP_MustBeReadOnly, argumentName));
        //}

        // IDbCommand.CommandType
        static internal ArgumentOutOfRangeException InvalidCommandType(CommandType value)
        {
#if DEBUG
            switch (value)
            {
                case CommandType.Text:
                case CommandType.StoredProcedure:
                case CommandType.TableDirect:
                    Debug.Assert(false, "valid CommandType " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(CommandType), (int)value);
        }

        static internal ArgumentOutOfRangeException InvalidConflictOptions(ConflictOption value)
        {
#if DEBUG
            switch (value)
            {
                case ConflictOption.CompareAllSearchableValues:
                case ConflictOption.CompareRowVersion:
                case ConflictOption.OverwriteChanges:
                    Debug.Assert(false, "valid ConflictOption " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(ConflictOption), (int)value);
        }

        // IDataAdapter.Update
        static internal ArgumentOutOfRangeException InvalidDataRowState(DataRowState value)
        {
#if DEBUG
            switch (value)
            {
                case DataRowState.Detached:
                case DataRowState.Unchanged:
                case DataRowState.Added:
                case DataRowState.Deleted:
                case DataRowState.Modified:
                    Debug.Assert(false, "valid DataRowState " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(DataRowState), (int)value);
        }

        // IDataParameter.SourceVersion
        static internal ArgumentOutOfRangeException InvalidDataRowVersion(DataRowVersion value)
        {
#if DEBUG
            switch (value)
            {
                case DataRowVersion.Default:
                case DataRowVersion.Current:
                case DataRowVersion.Original:
                case DataRowVersion.Proposed:
                    Debug.Assert(false, "valid DataRowVersion " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(DataRowVersion), (int)value);
        }

        // IDbConnection.BeginTransaction, OleDbTransaction.Begin
        static internal ArgumentOutOfRangeException InvalidIsolationLevel(IsolationLevel value)
        {
#if DEBUG
            switch (value)
            {
                case IsolationLevel.Unspecified:
                case IsolationLevel.Chaos:
                case IsolationLevel.ReadUncommitted:
                case IsolationLevel.ReadCommitted:
                case IsolationLevel.RepeatableRead:
                case IsolationLevel.Serializable:
                case IsolationLevel.Snapshot:
                    Debug.Assert(false, "valid IsolationLevel " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(IsolationLevel), (int)value);
        }

        // DBDataPermissionAttribute.KeyRestrictionBehavior
        static internal ArgumentOutOfRangeException InvalidKeyRestrictionBehavior(KeyRestrictionBehavior value)
        {
#if DEBUG
            switch (value)
            {
                case KeyRestrictionBehavior.PreventUsage:
                case KeyRestrictionBehavior.AllowOnly:
                    Debug.Assert(false, "valid KeyRestrictionBehavior " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(KeyRestrictionBehavior), (int)value);
        }

        // IDataAdapter.FillLoadOption
        static internal ArgumentOutOfRangeException InvalidLoadOption(LoadOption value)
        {
#if DEBUG
            switch (value)
            {
                case LoadOption.OverwriteChanges:
                case LoadOption.PreserveChanges:
                case LoadOption.Upsert:
                    Debug.Assert(false, "valid LoadOption " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(LoadOption), (int)value);
        }

        // IDataAdapter.MissingMappingAction
        static internal ArgumentOutOfRangeException InvalidMissingMappingAction(MissingMappingAction value)
        {
#if DEBUG
            switch (value)
            {
                case MissingMappingAction.Passthrough:
                case MissingMappingAction.Ignore:
                case MissingMappingAction.Error:
                    Debug.Assert(false, "valid MissingMappingAction " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(MissingMappingAction), (int)value);
        }

        // IDataAdapter.MissingSchemaAction
        static internal ArgumentOutOfRangeException InvalidMissingSchemaAction(MissingSchemaAction value)
        {
#if DEBUG
            switch (value)
            {
                case MissingSchemaAction.Add:
                case MissingSchemaAction.Ignore:
                case MissingSchemaAction.Error:
                case MissingSchemaAction.AddWithKey:
                    Debug.Assert(false, "valid MissingSchemaAction " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(MissingSchemaAction), (int)value);
        }

        // IDataParameter.Direction
        static internal ArgumentOutOfRangeException InvalidParameterDirection(ParameterDirection value)
        {
#if DEBUG
            switch (value)
            {
                case ParameterDirection.Input:
                case ParameterDirection.Output:
                case ParameterDirection.InputOutput:
                case ParameterDirection.ReturnValue:
                    Debug.Assert(false, "valid ParameterDirection " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(ParameterDirection), (int)value);
        }


        static internal ArgumentOutOfRangeException InvalidRule(Rule value)
        {
#if DEBUG
            switch (value)
            {
                case Rule.None:
                case Rule.Cascade:
                case Rule.SetNull:
                case Rule.SetDefault:
                    Debug.Assert(false, "valid Rule " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(Rule), (int)value);
        }

        // IDataAdapter.FillSchema
        static internal ArgumentOutOfRangeException InvalidSchemaType(SchemaType value)
        {
#if DEBUG
            switch (value)
            {
                case SchemaType.Source:
                case SchemaType.Mapped:
                    Debug.Assert(false, "valid SchemaType " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(SchemaType), (int)value);
        }

        // RowUpdatingEventArgs.StatementType
        static internal ArgumentOutOfRangeException InvalidStatementType(StatementType value)
        {
#if DEBUG
            switch (value)
            {
                case StatementType.Select:
                case StatementType.Insert:
                case StatementType.Update:
                case StatementType.Delete:
                case StatementType.Batch:
                    Debug.Assert(false, "valid StatementType " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(StatementType), (int)value);
        }

        // IDbCommand.UpdateRowSource
        static internal ArgumentOutOfRangeException InvalidUpdateRowSource(UpdateRowSource value)
        {
#if DEBUG
            switch (value)
            {
                case UpdateRowSource.None:
                case UpdateRowSource.OutputParameters:
                case UpdateRowSource.FirstReturnedRecord:
                case UpdateRowSource.Both:
                    Debug.Assert(false, "valid UpdateRowSource " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(UpdateRowSource), (int)value);
        }

        // RowUpdatingEventArgs.UpdateStatus
        static internal ArgumentOutOfRangeException InvalidUpdateStatus(UpdateStatus value)
        {
#if DEBUG
            switch (value)
            {
                case UpdateStatus.Continue:
                case UpdateStatus.ErrorsOccurred:
                case UpdateStatus.SkipAllRemainingRows:
                case UpdateStatus.SkipCurrentRow:
                    Debug.Assert(false, "valid UpdateStatus " + value.ToString());
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(UpdateStatus), (int)value);
        }

        static internal ArgumentOutOfRangeException NotSupportedCommandBehavior(CommandBehavior value, string method)
        {
            return NotSupportedEnumerationValue(typeof(CommandBehavior), value.ToString(), method);
        }

        static internal ArgumentOutOfRangeException NotSupportedStatementType(StatementType value, string method)
        {
            return NotSupportedEnumerationValue(typeof(StatementType), value.ToString(), method);
        }
        static private string ConnectionStateMsg(ConnectionState state)
        { // MDAC 82165, if the ConnectionState enum to msg the localization looks weird
            //switch (state)
            //{
            //    case (ConnectionState.Closed):
            //    case (ConnectionState.Connecting | ConnectionState.Broken): // treated the same as closed
            //        return Res.GetString(Res.ADP_ConnectionStateMsg_Closed);
            //    case (ConnectionState.Connecting):
            //        return Res.GetString(Res.ADP_ConnectionStateMsg_Connecting);
            //    case (ConnectionState.Open):
            //        return Res.GetString(Res.ADP_ConnectionStateMsg_Open);
            //    case (ConnectionState.Open | ConnectionState.Executing):
            //        return Res.GetString(Res.ADP_ConnectionStateMsg_OpenExecuting);
            //    case (ConnectionState.Open | ConnectionState.Fetching):
            //        return Res.GetString(Res.ADP_ConnectionStateMsg_OpenFetching);
            //    default:
            //        return Res.GetString(Res.ADP_ConnectionStateMsg, state.ToString());
            //}
            return state.ToString();
        }

        // IDbDataAdapter.Fill(Schema)
        static internal InvalidOperationException MissingSelectCommand(string method)
        {
            return Provider(string.Format("Missing select command in {0}", method));
        }

        //
        // AdapterMappingException
        //
        static private InvalidOperationException DataMapping(string error)
        {
            return InvalidOperation(error);
        }

        //// DataColumnMapping.GetDataColumnBySchemaAction
        static internal InvalidOperationException ColumnSchemaMissing(string cacheColumn, string tableName, string srcColumn)
        {
            if (ADP.IsEmpty(tableName))
            {
                // TODO: return InvalidOperation(Res.GetString(Res.ADP_ColumnSchemaMissing1, cacheColumn, tableName, srcColumn));
                return InvalidOperation("");
            }
            // TODO: return DataMapping(Res.GetString(Res.ADP_ColumnSchemaMissing2, cacheColumn, tableName, srcColumn));
            return DataMapping("");
        }

        // DbDataAdapter.Update
        static internal InvalidOperationException MissingTableMappingDestination(string dstTable)
        {
            return DataMapping(string.Format("Missing table mapping for destionation {0}", dstTable));
        }

        //
        // IDbCommand
        //

        //static internal InvalidOperationException CommandAsyncOperationCompleted()
        //{
        //    return InvalidOperation(Res.GetString(Res.SQL_AsyncOperationCompleted));
        //}

        static internal Exception CommandTextRequired(string method)
        {
            // return InvalidOperation(Res.GetString(Res.ADP_CommandTextRequired, method));
            return InvalidOperation("");
        }

        static internal InvalidOperationException UpdateConnectionRequired(StatementType statementType, bool isRowUpdatingCommand)
        {
            string resource;
            if (isRowUpdatingCommand)
            {
                resource = "Connection required for clone";
            }
            else
            {
                switch (statementType)
                {
                    case StatementType.Insert:
                        resource = "Connection required for insert";
                        break;
                    case StatementType.Update:
                        resource = "Connection required for update";
                        break;
                    case StatementType.Delete:
                        resource = "Connection required for delete";
                        break;
                    case StatementType.Batch:
                        resource = "Connection required for batch"; 
                        goto default;
#if DEBUG
                    case StatementType.Select:
                        Debug.Assert(false, "shouldn't be here");
                        goto default;
#endif
                    default:
                        throw ADP.InvalidStatementType(statementType);
                }
            }
            return InvalidOperation(resource);
        }

        static internal InvalidOperationException ConnectionRequired_Res(string method)
        {
            string resource = "ADP_ConnectionRequired_" + method;
//#if DEBUG
//            switch (resource)
//            {
//                case Res.ADP_ConnectionRequired_Fill:
//                case Res.ADP_ConnectionRequired_FillPage:
//                case Res.ADP_ConnectionRequired_FillSchema:
//                case Res.ADP_ConnectionRequired_Update:
//                case Res.ADP_ConnecitonRequired_UpdateRows:
//                    break;
//                default:
//                    Debug.Assert(false, "missing resource string: " + resource);
//                    break;
//            }
//#endif
            return InvalidOperation(resource);
        }
        static internal InvalidOperationException UpdateOpenConnectionRequired(StatementType statementType, bool isRowUpdatingCommand, ConnectionState state)
        {
            string resource;
            if (isRowUpdatingCommand)
            {
                resource = "Open connection required for clone";
            }
            else
            {
                switch (statementType)
                {
                    case StatementType.Insert:
                        resource = "Open connection required for insert";
                        break;
                    case StatementType.Update:
                        resource = "Open connection required for update";
                        break;
                    case StatementType.Delete:
                        resource = "Open connection required for delete";
                        break;
#if DEBUG
                    case StatementType.Select:
                        Debug.Assert(false, "shouldn't be here");
                        goto default;
                    case StatementType.Batch:
                        Debug.Assert(false, "isRowUpdatingCommand should have been true");
                        goto default;
#endif
                    default:
                        throw ADP.InvalidStatementType(statementType);
                }
            }
            return InvalidOperation(string.Format(resource, ADP.ConnectionStateMsg(state)));
        }

        //
        // DbDataAdapter
        //
        static internal ArgumentException UnwantedStatementType(StatementType statementType)
        {
            return Argument(string.Format("Unwanted statement type {0}", statementType.ToString()));
        }

        //
        // DbDataAdapter.FillSchema
        //
        static internal Exception FillSchemaRequiresSourceTableName(string parameter)
        {
            return Argument("Fill schema requires source table name", parameter);
        }

        //
        // DbDataAdapter.Fill
        //
        static internal Exception InvalidMaxRecords(string parameter, int max)
        {
            // TODO: return Argument(Res.GetString(Res.ADP_InvalidMaxRecords, max.ToString(CultureInfo.InvariantCulture)), parameter);
            return Argument(parameter);
        }
        static internal Exception InvalidStartRecord(string parameter, int start)
        {
            // TODO: return Argument(Res.GetString(Res.ADP_InvalidStartRecord, start.ToString(CultureInfo.InvariantCulture)), parameter);
            return Argument(parameter);
        }
        static internal Exception FillRequires(string parameter)
        {
            return ArgumentNull(parameter);
        }
        static internal Exception FillRequiresSourceTableName(string parameter)
        {
            // TODO: return Argument(Res.GetString(Res.ADP_FillRequiresSourceTableName), parameter);
            return Argument(parameter);
        }
        static internal Exception FillChapterAutoIncrement()
        {
            // TODO: return InvalidOperation(Res.GetString(Res.ADP_FillChapterAutoIncrement));
            return InvalidOperation("xx");
        }
        static internal InvalidOperationException MissingDataReaderFieldType(int index)
        {
            // TODO: return DataAdapter(Res.GetString(Res.ADP_MissingDataReaderFieldType, index));
            return DataAdapter(string.Format("xx{0}", index));
        }
        static internal InvalidOperationException OnlyOneTableForStartRecordOrMaxRecords()
        {
            // TODO: return DataAdapter(Res.GetString(Res.ADP_OnlyOneTableForStartRecordOrMaxRecords));
            return DataAdapter("yyy");
        }
        //
        // DbDataAdapter.Update
        //
        static internal ArgumentNullException UpdateRequiresNonNullDataSet(string parameter)
        {
            return ArgumentNull(parameter);
        }
        static internal InvalidOperationException UpdateRequiresSourceTable(string defaultSrcTableName)
        {
            // TODO: return InvalidOperation(Res.GetString(Res.ADP_UpdateRequiresSourceTable, defaultSrcTableName));
            return InvalidOperation("xxx");
        }
        static internal InvalidOperationException UpdateRequiresSourceTableName(string srcTable)
        {
            // TODO: return InvalidOperation(Res.GetString(Res.ADP_UpdateRequiresSourceTableName, srcTable)); // MDAC 70448
            return InvalidOperation("xxxx"); // MDAC 70448
        }
        static internal ArgumentNullException UpdateRequiresDataTable(string parameter)
        {
            return ArgumentNull(parameter);
        }

        static internal Exception UpdateConcurrencyViolation(StatementType statementType, int affected, int expected, DataRow[] dataRows)
        {
            string resource;
            switch (statementType)
            {
                case StatementType.Update:
                    resource = "Concurrency violation during update";
                    break;
                case StatementType.Delete:
                    resource = "Concurrency violation during delete";
                    break;
                case StatementType.Batch:
                    resource = "Concurrency violation during batch";
                    break;
#if DEBUG
                case StatementType.Select:
                case StatementType.Insert:
                    Debug.Assert(false, "should be here");
                    goto default;
#endif
                default:
                    throw ADP.InvalidStatementType(statementType);
            }
            // TODO: 
            DBConcurrencyException exception = new DBConcurrencyException(string.Format(resource, affected.ToString(CultureInfo.InvariantCulture), expected.ToString(CultureInfo.InvariantCulture)), null, dataRows);
            return exception;
        }

        static internal InvalidOperationException UpdateRequiresCommand(StatementType statementType, bool isRowUpdatingCommand)
        {
            string resource;
            if (isRowUpdatingCommand)
            {
                resource = "Update required command for clone";
            }
            else
            {
                switch (statementType)
                {
                    case StatementType.Select:
                        resource = "Update required command for select";
                        break;
                    case StatementType.Insert:
                        resource = "Update required command for insert";
                        break;
                    case StatementType.Update:
                        resource = "Update required command for update";
                        break;
                    case StatementType.Delete:
                        resource = "Update required command for delete";
                        break;
#if DEBUG
                    case StatementType.Batch:
                        Debug.Assert(false, "isRowUpdatingCommand should have been true");
                        goto default;
#endif
                    default:
                        throw ADP.InvalidStatementType(statementType);
                }
            }
            return InvalidOperation(resource);
        }
        static internal ArgumentException UpdateMismatchRowTable(int i)
        {
            // TODO: return Argument(Res.GetString(Res.ADP_UpdateMismatchRowTable, i.ToString(CultureInfo.InvariantCulture)));
            return Argument("");
        }
        static internal DataException RowUpdatedErrors()
        {
            // TODO: return Data(Res.GetString(Res.ADP_RowUpdatedErrors));
            return Data("");
        }
        static internal DataException RowUpdatingErrors()
        {
            // TODO: return Data(Res.GetString(Res.ADP_RowUpdatingErrors));
            return Data("");
        }
        static internal InvalidOperationException ResultsNotAllowedDuringBatch()
        {
            // TODO: return DataAdapter(Res.GetString(Res.ADP_ResultsNotAllowedDuringBatch));
            return DataAdapter("");
        }

        internal enum ConnectionError
        {
            BeginGetConnectionReturnsNull,
            GetConnectionReturnsNull,
            ConnectionOptionsMissing,
            CouldNotSwitchToClosedPreviouslyOpenedState,
        }

        internal enum InternalErrorCode
        {
            UnpooledObjectHasOwner = 0,
            UnpooledObjectHasWrongOwner = 1,
            PushingObjectSecondTime = 2,
            PooledObjectHasOwner = 3,
            PooledObjectInPoolMoreThanOnce = 4,
            CreateObjectReturnedNull = 5,
            NewObjectCannotBePooled = 6,
            NonPooledObjectUsedMoreThanOnce = 7,
            AttemptingToPoolOnRestrictedToken = 8,
            //          ConnectionOptionsInUse                                  =  9,
            ConvertSidToStringSidWReturnedNull = 10,
            //          UnexpectedTransactedObject                              = 11,
            AttemptingToConstructReferenceCollectionOnStaticObject = 12,
            AttemptingToEnlistTwice = 13,
            CreateReferenceCollectionReturnedNull = 14,
            PooledObjectWithoutPool = 15,
            UnexpectedWaitAnyResult = 16,
            SynchronousConnectReturnedPending = 17,
            CompletedConnectReturnedPending = 18,

            NameValuePairNext = 20,
            InvalidParserState1 = 21,
            InvalidParserState2 = 22,
            InvalidParserState3 = 23,

            InvalidBuffer = 30,

            UnimplementedSMIMethod = 40,
            InvalidSmiCall = 41,

            SqlDependencyObtainProcessDispatcherFailureObjectHandle = 50,
            SqlDependencyProcessDispatcherFailureCreateInstance = 51,
            SqlDependencyProcessDispatcherFailureAppDomain = 52,
            SqlDependencyCommandHashIsNotAssociatedWithNotification = 53,

            UnknownTransactionFailure = 60,
        }

        // global constant strings
        internal const string Append = "Append";
        internal const string BeginExecuteNonQuery = "BeginExecuteNonQuery";
        internal const string BeginExecuteReader = "BeginExecuteReader";
        internal const string BeginTransaction = "BeginTransaction";
        internal const string BeginExecuteXmlReader = "BeginExecuteXmlReader";
        internal const string ChangeDatabase = "ChangeDatabase";
        internal const string Cancel = "Cancel";
        internal const string Clone = "Clone";
        internal const string ColumnEncryptionSystemProviderNamePrefix = "MSSQL_";
        internal const string CommitTransaction = "CommitTransaction";
        internal const string CommandTimeout = "CommandTimeout";
        internal const string ConnectionString = "ConnectionString";
        internal const string DataSetColumn = "DataSetColumn";
        internal const string DataSetTable = "DataSetTable";
        internal const string Delete = "Delete";
        internal const string DeleteCommand = "DeleteCommand";
        internal const string DeriveParameters = "DeriveParameters";
        internal const string EndExecuteNonQuery = "EndExecuteNonQuery";
        internal const string EndExecuteReader = "EndExecuteReader";
        internal const string EndExecuteXmlReader = "EndExecuteXmlReader";
        internal const string ExecuteReader = "ExecuteReader";
        internal const string ExecuteRow = "ExecuteRow";
        internal const string ExecuteNonQuery = "ExecuteNonQuery";
        internal const string ExecuteScalar = "ExecuteScalar";
        internal const string ExecuteSqlScalar = "ExecuteSqlScalar";
        internal const string ExecuteXmlReader = "ExecuteXmlReader";
        internal const string Fill = "Fill";
        internal const string FillPage = "FillPage";
        internal const string FillSchema = "FillSchema";
        internal const string GetBytes = "GetBytes";
        internal const string GetChars = "GetChars";
        internal const string GetOleDbSchemaTable = "GetOleDbSchemaTable";
        internal const string GetProperties = "GetProperties";
        internal const string GetSchema = "GetSchema";
        internal const string GetSchemaTable = "GetSchemaTable";
        internal const string GetServerTransactionLevel = "GetServerTransactionLevel";
        internal const string Insert = "Insert";
        internal const string Open = "Open";
        internal const string Parameter = "Parameter";
        internal const string ParameterBuffer = "buffer";
        internal const string ParameterCount = "count";
        internal const string ParameterDestinationType = "destinationType";
        internal const string ParameterIndex = "index";
        internal const string ParameterName = "ParameterName";
        internal const string ParameterOffset = "offset";
        internal const string ParameterSetPosition = "set_Position";
        internal const string ParameterService = "Service";
        internal const string ParameterTimeout = "Timeout";
        internal const string ParameterUserData = "UserData";
        internal const string Prepare = "Prepare";
        internal const string QuoteIdentifier = "QuoteIdentifier";
        internal const string Read = "Read";
        internal const string ReadAsync = "ReadAsync";
        internal const string Remove = "Remove";
        internal const string RollbackTransaction = "RollbackTransaction";
        internal const string SaveTransaction = "SaveTransaction";
        internal const string SetProperties = "SetProperties";
        internal const string SourceColumn = "SourceColumn";
        internal const string SourceVersion = "SourceVersion";
        internal const string SourceTable = "SourceTable";
        internal const string UnquoteIdentifier = "UnquoteIdentifier";
        internal const string Update = "Update";
        internal const string UpdateCommand = "UpdateCommand";
        internal const string UpdateRows = "UpdateRows";

        internal const CompareOptions compareOptions = CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth | CompareOptions.IgnoreCase;
        internal const int DecimalMaxPrecision = 29;
        internal const int DecimalMaxPrecision28 = 28;  // there are some cases in Odbc where we need that ...
        internal const int DefaultCommandTimeout = 30;
        // internal const int DefaultConnectionTimeout = DbConnectionStringDefaults.ConnectTimeout;
        internal const float FailoverTimeoutStep = 0.08F;    // fraction of timeout to use for fast failover connections
        internal const int FirstTransparentAttemptTimeout = 500; // The first login attempt in  Transparent network IP Resolution 

        // security issue, don't rely upon static public readonly values - AS/URT 109635
        static internal readonly String StrEmpty = ""; // String.Empty

        static internal readonly IntPtr PtrZero = new IntPtr(0); // IntPtr.Zero
        static internal readonly int PtrSize = IntPtr.Size;
        static internal readonly IntPtr InvalidPtr = new IntPtr(-1); // use for INVALID_HANDLE
        static internal readonly IntPtr RecordsUnaffected = new IntPtr(-1);

        static internal readonly HandleRef NullHandleRef = new HandleRef(null, IntPtr.Zero);

        internal const int CharSize = System.Text.UnicodeEncoding.CharSize;

        static internal bool CompareInsensitiveInvariant(string strvalue, string strconst)
        {
            return (0 == CultureInfo.InvariantCulture.CompareInfo.Compare(strvalue, strconst, CompareOptions.IgnoreCase));
        }

        static internal Delegate FindBuilder(MulticastDelegate mcd)
        { // V1.2.3300
            if (null != mcd)
            {
                Delegate[] d = mcd.GetInvocationList();
                for (int i = 0; i < d.Length; i++)
                {
                    if (d[i].Target is DbCommandBuilder)
                        return d[i];
                }
            }

            return null;
        }

        static internal readonly bool IsWindowsNT = (PlatformID.Win32NT == Environment.OSVersion.Platform);
        static internal readonly bool IsPlatformNT5 = (ADP.IsWindowsNT && (Environment.OSVersion.Version.Major >= 5));

        static internal long TimerCurrent()
        {
            return DateTime.UtcNow.ToFileTimeUtc();
        }

        static internal long TimerFromSeconds(int seconds)
        {
            long result = checked((long)seconds * TimeSpan.TicksPerSecond);
            return result;
        }

        static internal long TimerFromMilliseconds(long milliseconds)
        {
            long result = checked(milliseconds * TimeSpan.TicksPerMillisecond);
            return result;
        }

        static internal bool TimerHasExpired(long timerExpire)
        {
            bool result = TimerCurrent() > timerExpire;
            return result;
        }

        static internal long TimerRemaining(long timerExpire)
        {
            long timerNow = TimerCurrent();
            long result = checked(timerExpire - timerNow);
            return result;
        }

        static internal long TimerRemainingMilliseconds(long timerExpire)
        {
            long result = TimerToMilliseconds(TimerRemaining(timerExpire));
            return result;
        }

        static internal long TimerRemainingSeconds(long timerExpire)
        {
            long result = TimerToSeconds(TimerRemaining(timerExpire));
            return result;
        }

        static internal long TimerToMilliseconds(long timerValue)
        {
            long result = timerValue / TimeSpan.TicksPerMillisecond;
            return result;
        }

        static private long TimerToSeconds(long timerValue)
        {
            long result = timerValue / TimeSpan.TicksPerSecond;
            return result;
        }

        static internal string BuildQuotedString(string quotePrefix, string quoteSuffix, string unQuotedString)
        {
            StringBuilder resultString = new StringBuilder();
            if (ADP.IsEmpty(quotePrefix) == false)
            {
                resultString.Append(quotePrefix);
            }

            // Assuming that the suffix is escaped by doubling it. i.e. foo"bar becomes "foo""bar".
            if (ADP.IsEmpty(quoteSuffix) == false)
            {
                resultString.Append(unQuotedString.Replace(quoteSuffix, quoteSuffix + quoteSuffix));
                resultString.Append(quoteSuffix);
            }
            else
            {
                resultString.Append(unQuotedString);
            }

            return resultString.ToString();
        }

        static internal DataRow[] SelectAdapterRows(DataTable dataTable, bool sorted)
        {
            const DataRowState rowStates = DataRowState.Added | DataRowState.Deleted | DataRowState.Modified;

            // equivalent to but faster than 'return dataTable.Select("", "", rowStates);'
            int countAdded = 0, countDeleted = 0, countModifed = 0;
            DataRowCollection rowCollection = dataTable.Rows;
            foreach (DataRow dataRow in rowCollection)
            {
                switch (dataRow.RowState)
                {
                    case DataRowState.Added:
                        countAdded++;
                        break;
                    case DataRowState.Deleted:
                        countDeleted++;
                        break;
                    case DataRowState.Modified:
                        countModifed++;
                        break;
                    default:
                        Debug.Assert(0 == (rowStates & dataRow.RowState), "flagged RowState");
                        break;
                }
            }
            DataRow[] dataRows = new DataRow[countAdded + countDeleted + countModifed];
            if (sorted)
            {
                countModifed = countAdded + countDeleted;
                countDeleted = countAdded;
                countAdded = 0;

                foreach (DataRow dataRow in rowCollection)
                {
                    switch (dataRow.RowState)
                    {
                        case DataRowState.Added:
                            dataRows[countAdded++] = dataRow;
                            break;
                        case DataRowState.Deleted:
                            dataRows[countDeleted++] = dataRow;
                            break;
                        case DataRowState.Modified:
                            dataRows[countModifed++] = dataRow;
                            break;
                        default:
                            Debug.Assert(0 == (rowStates & dataRow.RowState), "flagged RowState");
                            break;
                    }
                }
            }
            else
            {
                int index = 0;
                foreach (DataRow dataRow in rowCollection)
                {
                    if (0 != (dataRow.RowState & rowStates))
                    {
                        dataRows[index++] = dataRow;
                        if (index == dataRows.Length)
                        {
                            break;
                        }
                    }
                }
            }
            return dataRows;
        }

        internal static int StringLength(string inputString)
        {
            return ((null != inputString) ? inputString.Length : 0);
        }

        // { "a", "a", "a" } -> { "a", "a1", "a2" }
        // { "a", "a", "a1" } -> { "a", "a2", "a1" }
        // { "a", "A", "a" } -> { "a", "A1", "a2" }
        // { "a", "A", "a1" } -> { "a", "A2", "a1" } // MDAC 66718
        static internal void BuildSchemaTableInfoTableNames(string[] columnNameArray)
        {
            Dictionary<string, int> hash = new Dictionary<string, int>(columnNameArray.Length);

            int startIndex = columnNameArray.Length; // lowest non-unique index
            for (int i = columnNameArray.Length - 1; 0 <= i; --i)
            {
                string columnName = columnNameArray[i];
                if ((null != columnName) && (0 < columnName.Length))
                {
                    columnName = columnName.ToLower(CultureInfo.InvariantCulture);
                    int index;
                    if (hash.TryGetValue(columnName, out index))
                    {
                        startIndex = Math.Min(startIndex, index);
                    }
                    hash[columnName] = i;
                }
                else
                {
                    columnNameArray[i] = ADP.StrEmpty; // MDAC 66681
                    startIndex = i;
                }
            }
            int uniqueIndex = 1;
            for (int i = startIndex; i < columnNameArray.Length; ++i)
            {
                string columnName = columnNameArray[i];
                if (0 == columnName.Length)
                { // generate a unique name
                    columnNameArray[i] = "Column";
                    uniqueIndex = GenerateUniqueName(hash, ref columnNameArray[i], i, uniqueIndex);
                }
                else
                {
                    columnName = columnName.ToLower(CultureInfo.InvariantCulture);
                    if (i != hash[columnName])
                    {
                        GenerateUniqueName(hash, ref columnNameArray[i], i, 1); // MDAC 66718
                    }
                }
            }
        }

        static private int GenerateUniqueName(Dictionary<string, int> hash, ref string columnName, int index, int uniqueIndex)
        {
            for (; ; ++uniqueIndex)
            {
                string uniqueName = columnName + uniqueIndex.ToString(CultureInfo.InvariantCulture);
                string lowerName = uniqueName.ToLower(CultureInfo.InvariantCulture); // MDAC 66978
                if (!hash.ContainsKey(lowerName))
                {

                    columnName = uniqueName;
                    hash.Add(lowerName, index);
                    break;
                }
            }
            return uniqueIndex;
        }

        static internal int IntPtrToInt32(IntPtr value)
        {
            if (4 == ADP.PtrSize)
            {
                return (int)value;
            }
            else
            {
                long lval = (long)value;
                lval = Math.Min((long)Int32.MaxValue, lval);
                lval = Math.Max((long)Int32.MinValue, lval);
                return (int)lval;
            }
        }

        // 
        static internal int SrcCompare(string strA, string strB)
        { // this is null safe
            return ((strA == strB) ? 0 : 1);
        }

        static internal int DstCompare(string strA, string strB)
        { // this is null safe
            return CultureInfo.CurrentCulture.CompareInfo.Compare(strA, strB, ADP.compareOptions);
        }

        static internal bool IsDirection(IDataParameter value, ParameterDirection condition)
        {
#if DEBUG
            IsDirectionValid(condition);
#endif
            return (condition == (condition & value.Direction));
        }
#if DEBUG
        static private void IsDirectionValid(ParameterDirection value)
        {
            switch (value)
            { // @perfnote: Enum.IsDefined
                case ParameterDirection.Input:
                case ParameterDirection.Output:
                case ParameterDirection.InputOutput:
                case ParameterDirection.ReturnValue:
                    break;
                default:
                    throw ADP.InvalidParameterDirection(value);
            }
        }
#endif

        static internal bool IsEmpty(string str)
        {
            return ((null == str) || (0 == str.Length));
        }

        static internal bool IsEmptyArray(string[] array)
        {
            return ((null == array) || (0 == array.Length));
        }

        static internal bool IsNull(object value)
        {
            if ((null == value) || (DBNull.Value == value))
            {
                return true;
            }

            var nullable = value as INullable;
            return ((nullable != null) && nullable.IsNull);
        }
    }
}