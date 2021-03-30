using System;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Security;
using System.Threading;

namespace AsyncDataAdapter.Internal
{
    //  public static partial class Validation
    public static partial class ADP
    {
        // only StackOverflowException & ThreadAbortException are sealed classes
        private static readonly Type _StackOverflowType   = typeof(StackOverflowException);
        private static readonly Type _OutOfMemoryType     = typeof(OutOfMemoryException);
        private static readonly Type _ThreadAbortType     = typeof(ThreadAbortException);
        private static readonly Type _NullReferenceType   = typeof(NullReferenceException);
        private static readonly Type _AccessViolationType = typeof(AccessViolationException);
        private static readonly Type _SecurityType        = typeof(SecurityException);

        public static bool IsCatchableExceptionType(Exception e)
        {
            // a 'catchable' exception is defined by what it is not.
            Debug.Assert(e != null, "Unexpected null exception!");
            Type type = e.GetType();

            return
                (type != _StackOverflowType) &&
                (type != _OutOfMemoryType) &&
                (type != _ThreadAbortType) &&
                (type != _NullReferenceType) &&
                (type != _AccessViolationType) &&
                !_SecurityType.IsAssignableFrom(type);
        }

        public static bool IsCatchableOrSecurityExceptionType(Exception e)
        {
            // a 'catchable' exception is defined by what it is not.
            // since IsCatchableExceptionType defined SecurityException as not 'catchable'
            // this method will return true for SecurityException has being catchable.

            // the other way to write this method is, but then SecurityException is checked twice
            // return ((e is SecurityException) || IsCatchableExceptionType(e));

            Debug.Assert(e != null, "Unexpected null exception!");
            Type type = e.GetType();

            return (type != _StackOverflowType) &&
                     (type != _OutOfMemoryType) &&
                     (type != _ThreadAbortType) &&
                     (type != _NullReferenceType) &&
                     (type != _AccessViolationType);
        }

        // Invalid Enumeration

        public static ArgumentOutOfRangeException InvalidEnumerationValue(Type type, int value)
        {
            string msg = string.Format("The {0} enumeration value, {1}, is invalid.", type.Name, value.ToString(CultureInfo.InvariantCulture));
            return new ArgumentOutOfRangeException(paramName: type.Name, actualValue: value, message: msg);
        }

        public static ArgumentOutOfRangeException NotSupportedEnumerationValue(Type type, string value, string method)
        {
            string msg = string.Format("The {0} enumeration value, {1}, is not supported by the {2} method.", type.Name, value.ToString(CultureInfo.InvariantCulture), method);
            return new ArgumentOutOfRangeException(paramName: type.Name, actualValue: value, message: msg);
        }

        // IDataAdapter.Update
        public static ArgumentOutOfRangeException InvalidDataRowState(DataRowState value)
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

        // IDataAdapter.FillLoadOption
        public static ArgumentOutOfRangeException InvalidLoadOption(LoadOption value)
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
        public static ArgumentOutOfRangeException InvalidMissingMappingAction(MissingMappingAction value)
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
        public static ArgumentOutOfRangeException InvalidMissingSchemaAction(MissingSchemaAction value)
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
        public static ArgumentOutOfRangeException InvalidParameterDirection(ParameterDirection value)
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

        // IDataAdapter.FillSchema
        public static ArgumentOutOfRangeException InvalidSchemaType(SchemaType value)
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
        public static ArgumentOutOfRangeException InvalidStatementType(StatementType value)
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

        // RowUpdatingEventArgs.UpdateStatus
        public static ArgumentOutOfRangeException InvalidUpdateStatus(UpdateStatus value)
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

        public static ArgumentOutOfRangeException NotSupportedCommandBehavior(CommandBehavior value, string method)
        {
            return NotSupportedEnumerationValue(typeof(CommandBehavior), value.ToString(), method);
        }

        public static string ConnectionStateMsg(ConnectionState state)
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
        public static InvalidOperationException MissingSelectCommand(string method)
        {
            return new InvalidOperationException(string.Format("Missing select command in {0}", method));
        }

        //
        // AdapterMappingException
        //
        private static InvalidOperationException DataMapping(string error)
        {
            return new InvalidOperationException(error);
        }

        // DbDataAdapter.Update
        public static InvalidOperationException MissingTableMappingDestination(string dstTable)
        {
            return DataMapping(string.Format("Missing table mapping for destionation {0}", dstTable));
        }

        //
        // IDbCommand
        //

        public static InvalidOperationException UpdateConnectionRequired(StatementType statementType, bool isRowUpdatingCommand)
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
            return new InvalidOperationException(resource);
        }

        public static InvalidOperationException UpdateOpenConnectionRequired(StatementType statementType, bool isRowUpdatingCommand, ConnectionState state)
        {
            string message;
            if (isRowUpdatingCommand)
            {
                message = "Open connection required for clone";
            }
            else
            {
                switch (statementType)
                {
                    case StatementType.Insert:
                        message = "Open connection required for insert";
                        break;
                    case StatementType.Update:
                        message = "Open connection required for update";
                        break;
                    case StatementType.Delete:
                        message = "Open connection required for delete";
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

            return new InvalidOperationException(string.Format(message, ADP.ConnectionStateMsg(state)));
        }

        //
        // DbDataAdapter
        //

        //
        // DbDataAdapter.FillSchema
        //
        public static Exception FillSchemaRequiresSourceTableName(string parameter)
        {
            return new ArgumentException("Fill schema requires source table name", parameter);
        }

        //
        // DbDataAdapter.Fill
        //
        public static Exception InvalidMaxRecords(string parameter, int max)
        {
            return new ArgumentException(message: string.Format("The MaxRecords value of {0} is invalid; the value must be >= 0.", max), parameter);
        }
        public static Exception InvalidStartRecord(string parameter, int start)
        {
            return new ArgumentException(message: string.Format("The StartRecord value of {0} is invalid; the value must be >= 0.", start), parameter);
        }
        public static Exception FillRequires(string parameter)
        {
            return new ArgumentNullException(parameter);
        }
        public static Exception FillRequiresSourceTableName(string parameter)
        {
            return new ArgumentException(message: "Fill: expected a non-empty string for the SourceTable name.", paramName: parameter);
        }
        public static Exception FillChapterAutoIncrement()
        {
            return new InvalidOperationException("Hierarchical chapter columns must map to an AutoIncrement DataColumn.");
        }
        public static InvalidOperationException MissingDataReaderFieldType(int index)
        {
            return new InvalidOperationException(string.Format("DataReader.GetFieldType({0}) returned null.", index));
        }
        public static InvalidOperationException OnlyOneTableForStartRecordOrMaxRecords()
        {
            return new InvalidOperationException("Only specify one item in the dataTables array when using non-zero values for startRecords or maxRecords.");
        }

        public static Exception UpdateConcurrencyViolation(StatementType statementType, int affected, int expected, DataRow[] dataRows)
        {
            string format;
            switch (statementType)
            {
                case StatementType.Update:
                    format = "Concurrency violation during update";
                    break;
                case StatementType.Delete:
                    format = "Concurrency violation during delete";
                    break;
                case StatementType.Batch:
                    format = "Concurrency violation during batch";
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
            
            string msg = string.Format(CultureInfo.CurrentCulture, format, affected, expected);
            DBConcurrencyException exception = new DBConcurrencyException(message: msg, inner: null, dataRows: dataRows);
            return exception;
        }

        public static InvalidOperationException UpdateRequiresCommand(StatementType statementType, bool isRowUpdatingCommand)
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
            return new InvalidOperationException(resource);
        }
    }
}
