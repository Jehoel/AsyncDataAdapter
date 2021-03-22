using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.Security;
using System.Threading;

namespace AsyncDataAdapter
{
    internal static class ADP
    {
        // only StackOverflowException & ThreadAbortException are sealed classes
        private static readonly Type _StackOverflowType   = typeof(StackOverflowException);
        private static readonly Type _OutOfMemoryType     = typeof(OutOfMemoryException);
        private static readonly Type _ThreadAbortType     = typeof(ThreadAbortException);
        private static readonly Type _NullReferenceType   = typeof(NullReferenceException);
        private static readonly Type _AccessViolationType = typeof(AccessViolationException);
        private static readonly Type _SecurityType        = typeof(SecurityException);

        internal static bool IsCatchableExceptionType(Exception e)
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

        internal static bool IsCatchableOrSecurityExceptionType(Exception e)
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

        internal static ArgumentOutOfRangeException InvalidEnumerationValue(Type type, int value)
        {
            string msg = string.Format("The {0} enumeration value, {1}, is invalid.", type.Name, value.ToString(CultureInfo.InvariantCulture));
            return new ArgumentOutOfRangeException(paramName: type.Name, actualValue: value, message: msg);
        }

        internal static ArgumentOutOfRangeException NotSupportedEnumerationValue(Type type, string value, string method)
        {
            string msg = string.Format("The {0} enumeration value, {1}, is not supported by the {2} method.", type.Name, value.ToString(CultureInfo.InvariantCulture), method);
            return new ArgumentOutOfRangeException(paramName: type.Name, actualValue: value, message: msg);
        }

        // IDataAdapter.Update
        internal static ArgumentOutOfRangeException InvalidDataRowState(DataRowState value)
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
        internal static ArgumentOutOfRangeException InvalidLoadOption(LoadOption value)
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
        internal static ArgumentOutOfRangeException InvalidMissingMappingAction(MissingMappingAction value)
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
        internal static ArgumentOutOfRangeException InvalidMissingSchemaAction(MissingSchemaAction value)
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
        internal static ArgumentOutOfRangeException InvalidParameterDirection(ParameterDirection value)
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
        internal static ArgumentOutOfRangeException InvalidSchemaType(SchemaType value)
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
        internal static ArgumentOutOfRangeException InvalidStatementType(StatementType value)
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
        internal static ArgumentOutOfRangeException InvalidUpdateStatus(UpdateStatus value)
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

        internal static ArgumentOutOfRangeException NotSupportedCommandBehavior(CommandBehavior value, string method)
        {
            return NotSupportedEnumerationValue(typeof(CommandBehavior), value.ToString(), method);
        }

        private static string ConnectionStateMsg(ConnectionState state)
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
        internal static InvalidOperationException MissingSelectCommand(string method)
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
        internal static InvalidOperationException MissingTableMappingDestination(string dstTable)
        {
            return DataMapping(string.Format("Missing table mapping for destionation {0}", dstTable));
        }

        //
        // IDbCommand
        //

        internal static InvalidOperationException UpdateConnectionRequired(StatementType statementType, bool isRowUpdatingCommand)
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

        internal static InvalidOperationException ConnectionRequired_Res(string method)
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
            return new InvalidOperationException(resource);
        }
        internal static InvalidOperationException UpdateOpenConnectionRequired(StatementType statementType, bool isRowUpdatingCommand, ConnectionState state)
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
        internal static Exception FillSchemaRequiresSourceTableName(string parameter)
        {
            return new ArgumentException("Fill schema requires source table name", parameter);
        }

        //
        // DbDataAdapter.Fill
        //
        internal static Exception InvalidMaxRecords(string parameter, int max)
        {
            return new ArgumentException(message: string.Format("The MaxRecords value of {0} is invalid; the value must be >= 0.", max), parameter);
        }
        internal static Exception InvalidStartRecord(string parameter, int start)
        {
            return new ArgumentException(message: string.Format("The StartRecord value of {0} is invalid; the value must be >= 0.", start), parameter);
        }
        internal static Exception FillRequires(string parameter)
        {
            return new ArgumentNullException(parameter);
        }
        internal static Exception FillRequiresSourceTableName(string parameter)
        {
            return new ArgumentException(message: "Fill: expected a non-empty string for the SourceTable name.", paramName: parameter);
        }
        internal static Exception FillChapterAutoIncrement()
        {
            return new InvalidOperationException("Hierarchical chapter columns must map to an AutoIncrement DataColumn.");
        }
        internal static InvalidOperationException MissingDataReaderFieldType(int index)
        {
            return new InvalidOperationException(string.Format("DataReader.GetFieldType({0}) returned null.", index));
        }
        internal static InvalidOperationException OnlyOneTableForStartRecordOrMaxRecords()
        {
            return new InvalidOperationException("Only specify one item in the dataTables array when using non-zero values for startRecords or maxRecords.");
        }

        internal static Exception UpdateConcurrencyViolation(StatementType statementType, int affected, int expected, DataRow[] dataRows)
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

        internal static InvalidOperationException UpdateRequiresCommand(StatementType statementType, bool isRowUpdatingCommand)
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

        internal static Delegate FindBuilder(MulticastDelegate mcd)
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

        internal static DataRow[] SelectAdapterRows(DataTable dataTable, bool sorted)
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

        // { "a", "a", "a" } -> { "a", "a1", "a2" }
        // { "a", "a", "a1" } -> { "a", "a2", "a1" }
        // { "a", "A", "a" } -> { "a", "A1", "a2" }
        // { "a", "A", "a1" } -> { "a", "A2", "a1" } // MDAC 66718
        internal static void BuildSchemaTableInfoTableNames(string[] columnNameArray)
        {
            Dictionary<string, int> hash = new Dictionary<string, int>(columnNameArray.Length);

            int startIndex = columnNameArray.Length; // lowest non-unique index
            for (int i = columnNameArray.Length - 1; 0 <= i; --i)
            {
                string columnName = columnNameArray[i];
                if ((null != columnName) && (0 < columnName.Length))
                {
                    columnName = columnName.ToLower(CultureInfo.InvariantCulture);
                    if (hash.TryGetValue(columnName, out int index))
                    {
                        startIndex = Math.Min(startIndex, index);
                    }
                    hash[columnName] = i;
                }
                else
                {
                    columnNameArray[i] = String.Empty;
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
                        _ = GenerateUniqueName(hash, ref columnNameArray[i], i, 1); // MDAC 66718
                    }
                }
            }
        }

        private static int GenerateUniqueName(Dictionary<string, int> hash, ref string columnName, int index, int uniqueIndex)
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

        /// <summary>Indicates if <paramref name="value"/> is <see langword="null"/>, <see cref="DBNull"/>, or if it's <see cref="INullable"/> and indicates yes.</summary>
        /// <param name="value"></param>
        /// <returns></returns>
        internal static bool IsNull(object value)
        {
            if( value is null )
            {
                return true;
            }
            else if( Object.ReferenceEquals( value, DBNull.Value ) )
            {
                return true;
            }
            else if( value is INullable nullable )
            {
                return nullable.IsNull;
            }
            else
            {
                return false;
            }
        }
    }
}
