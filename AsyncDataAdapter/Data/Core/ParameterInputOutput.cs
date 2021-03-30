using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace AsyncDataAdapter.Internal
{
    public static class ParameterMethods
    {
        public static void ParameterInput( MissingMappingAction missingMapping, MissingSchemaAction missingSchema, IDataParameterCollection parameters, StatementType typeIndex, DataRow row, DataTableMapping mappings)
        {
            foreach (IDataParameter parameter in parameters)
            {
                if ((null != parameter) && (0 != (ParameterDirection.Input & parameter.Direction)))
                {
                    string columnName = parameter.SourceColumn;
                    if (!string.IsNullOrEmpty(columnName))
                    {
                        DataColumn dataColumn = mappings.GetDataColumn( sourceColumn: columnName, dataType: null, dataTable: row.Table, missingMapping, missingSchema );
                        if (null != dataColumn)
                        {
                            DataRowVersion version = GetParameterSourceVersion(typeIndex, parameter);
                            parameter.Value = row[dataColumn, version];
                        }
                        else
                        {
                            parameter.Value = null;
                        }

                        if( parameter is DbParameter p2 && p2.SourceColumnNullMapping )
                        {
                            Debug.Assert(DbType.Int32 == parameter.DbType, "unexpected DbType");
                            parameter.Value = Utility.IsNull( parameter.Value ) ? ParameterValueNullValue : ParameterValueNonNullValue;
                        }
                    }
                }
            }
        }

        internal static readonly object ParameterValueNonNullValue = 0;
        internal static readonly object ParameterValueNullValue    = 1;

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

        public static void ParameterOutput( MissingMappingAction missingMapping, MissingSchemaAction missingSchema, IDataParameterCollection parameters, DataRow row, DataTableMapping mappings )
        {
            foreach (IDataParameter parameter in parameters)
            {
                if (null != parameter)
                {
                    ParameterOutput( parameter, row, mappings, missingMapping, missingSchema );
                }
            }
        }

        public static void ParameterOutput( IDataParameter parameter, DataRow row, DataTableMapping mappings, MissingMappingAction missingMapping, MissingSchemaAction missingSchema )
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
                        DataColumn dataColumn = mappings.GetDataColumn( sourceColumn: columnName, dataType: null, dataTable: row.Table, missingMapping, missingSchema );
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
    }
}
