using System;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Globalization;

namespace AsyncDataAdapter.Internal
{
    public sealed class AdaDbSchemaRow
    {
        internal const string SchemaMappingUnsortedIndex = "SchemaMapping Unsorted Index";
        private readonly AdaDbSchemaTable schemaTable;
        private readonly DataRow dataRow;

        internal static AdaDbSchemaRow[] GetSortedSchemaRows(DataTable dataTable, bool returnProviderSpecificTypes)
        { // MDAC 60609
            DataColumn sortindex = dataTable.Columns[SchemaMappingUnsortedIndex];
            if (null == sortindex)
            { // WebData 100390
                sortindex = new DataColumn(SchemaMappingUnsortedIndex, typeof(Int32)); // MDAC 67050
                dataTable.Columns.Add(sortindex);
            }
            int count = dataTable.Rows.Count;
            for (int i = 0; i < count; ++i)
            {
                dataTable.Rows[i][sortindex] = i;
            };
            AdaDbSchemaTable schemaTable = new AdaDbSchemaTable(dataTable, returnProviderSpecificTypes);

            const DataViewRowState rowStates = DataViewRowState.Unchanged | DataViewRowState.Added | DataViewRowState.ModifiedCurrent;
            DataRow[] dataRows = dataTable.Select(null, "ColumnOrdinal ASC", rowStates);
            Debug.Assert(null != dataRows, "GetSchemaRows: unexpected null dataRows");

            AdaDbSchemaRow[] schemaRows = new AdaDbSchemaRow[dataRows.Length];

            for (int i = 0; i < dataRows.Length; ++i)
            {
                schemaRows[i] = new AdaDbSchemaRow(schemaTable, dataRows[i]);
            }
            return schemaRows;
        }

        internal AdaDbSchemaRow(AdaDbSchemaTable schemaTable, DataRow dataRow)
        {
            this.schemaTable = schemaTable;
            this.dataRow = dataRow;
        }

        internal DataRow DataRow => this.dataRow;

        internal string ColumnName
        {
            get
            {
                Debug.Assert(null != this.schemaTable.ColumnName, "no column ColumnName");
                object value = this.dataRow[this.schemaTable.ColumnName, DataRowVersion.Default];
                if (!Convert.IsDBNull(value))
                {
                    return Convert.ToString(value, CultureInfo.InvariantCulture);
                }
                return "";
            }
            /*set {
                Debug.Assert(null != schemaTable.ColumnName, "missing column ColumnName");
                dataRow[schemaTable.ColumnName] = value;
            }*/
        }

        //internal Int32 Ordinal {
        /*get {
            Debug.Assert(null != schemaTable.Ordinal, "no column Ordinal");
            return Convert.ToInt32(dataRow[schemaTable.Ordinal, DataRowVersion.Default], CultureInfo.InvariantCulture);
        }*/
        /*set {
            Debug.Assert(null != schemaTable.Ordinal, "missing column Ordinal");
            dataRow[schemaTable.Ordinal] = value;
        }*/

        //}

        internal Int32 Size
        {
            get
            {
                Debug.Assert(null != this.schemaTable.Size, "no column Size");
                object value = this.dataRow[this.schemaTable.Size, DataRowVersion.Default];
                if (!Convert.IsDBNull(value))
                {
                    return Convert.ToInt32(value, CultureInfo.InvariantCulture);
                }
                return 0;
            }
            /*set {
                Debug.Assert(null != schemaTable.Size, "missing column Size");
                dataRow[schemaTable.Size] = value;
            }*/
        }

        internal string BaseColumnName
        {
            get
            {
                if (null != this.schemaTable.BaseColumnName)
                {
                    object value = this.dataRow[this.schemaTable.BaseColumnName, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToString(value, CultureInfo.InvariantCulture);
                    }
                }
                return "";
            }
            /*set {
                Debug.Assert(null != schemaTable.BaseColumnName, "missing column BaseColumnName");
                dataRow[schemaTable.BaseColumnName] = value;
            }*/
        }

        internal string BaseServerName
        {
            get
            {
                if (null != this.schemaTable.BaseServerName)
                {
                    object value = this.dataRow[this.schemaTable.BaseServerName, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToString(value, CultureInfo.InvariantCulture);
                    }
                }
                return "";
            }
            /*set {
                Debug.Assert(null != schemaTable.BaseServerName, "missing column BaseServerName");
                dataRow[schemaTable.BaseServerName] = value;
            }*/
        }


        internal string BaseCatalogName
        {
            get
            {
                if (null != this.schemaTable.BaseCatalogName)
                {
                    object value = this.dataRow[this.schemaTable.BaseCatalogName, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToString(value, CultureInfo.InvariantCulture);
                    }
                }
                return "";
            }
            /*set {
                Debug.Assert(null != schemaTable.BaseCatalogName, "missing column BaseCatalogName");
                dataRow[schemaTable.BaseCatalogName] = value;
            }*/
        }

        internal string BaseSchemaName
        {
            get
            {
                if (null != this.schemaTable.BaseSchemaName)
                {
                    object value = this.dataRow[this.schemaTable.BaseSchemaName, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToString(value, CultureInfo.InvariantCulture);
                    }
                }
                return "";
            }
            /*set {
                Debug.Assert(null != schemaTable.BaseSchemaName, "missing column BaseSchemaName");
                dataRow[schemaTable.BaseSchemaName] = value;
            }*/
        }

        internal string BaseTableName
        {
            get
            {
                if (null != this.schemaTable.BaseTableName)
                {
                    object value = this.dataRow[this.schemaTable.BaseTableName, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToString(value, CultureInfo.InvariantCulture);
                    }
                }
                return "";
            }
            /*set {
                Debug.Assert(null != schemaTable.BaseTableName, "missing column BaseTableName");
                dataRow[schemaTable.BaseTableName] = value;
            }*/
        }

        internal bool IsAutoIncrement
        {
            get
            {
                if (null != this.schemaTable.IsAutoIncrement)
                {
                    object value = this.dataRow[this.schemaTable.IsAutoIncrement, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return false;
            }
            /*set {
                Debug.Assert(null != schemaTable.IsAutoIncrement, "missing column IsAutoIncrement");
                dataRow[schemaTable.IsAutoIncrement] = (bool)value;
            }*/
        }

        internal bool IsUnique
        {
            get
            {
                if (null != this.schemaTable.IsUnique)
                {
                    object value = this.dataRow[this.schemaTable.IsUnique, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return false;
            }
            /*set {
                Debug.Assert(null != schemaTable.IsUnique, "missing column IsUnique");
                dataRow[schemaTable.IsUnique] = (bool)value;
            }*/
        }

        internal bool IsRowVersion
        {
            get
            {
                if (null != this.schemaTable.IsRowVersion)
                {
                    object value = this.dataRow[this.schemaTable.IsRowVersion, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return false;
            }
            /*set {
                Debug.Assert(null != schemaTable.IsRowVersion, "missing column IsRowVersion");
                dataRow[schemaTable.IsRowVersion] = value;
            }*/
        }

        internal bool IsKey
        {
            get
            {
                if (null != this.schemaTable.IsKey)
                {
                    object value = this.dataRow[this.schemaTable.IsKey, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return false;
            }
            /*set {
                Debug.Assert(null != schemaTable.IsKey, "missing column IsKey");
                dataRow[schemaTable.IsKey] = value;
            }*/
        }

        // consider:  just do comparison directly -> (object)(baseColumnName) == (object)(columnName)
        //internal bool IsAliased {
        /*get {
            if (null != schemaTable.IsAliased) { // MDAC 62336
                object value = dataRow[schemaTable.IsAliased, DataRowVersion.Default];
                if (!Convert.IsDBNull(value)) {
                    return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                }
            }
            return false;
        }*/
        /*set {
            Debug.Assert(null != schemaTable.IsAliased, "missing column IsAliased");
            dataRow[schemaTable.IsAliased] = value;
        }*/
        //}

        internal bool IsExpression
        {
            get
            {
                if (null != this.schemaTable.IsExpression)
                { // MDAC 62336
                    object value = this.dataRow[this.schemaTable.IsExpression, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return false;
            }
            /*set {
                Debug.Assert(null != schemaTable.IsExpression, "missing column IsExpression");
                dataRow[schemaTable.IsExpression] = value;
            }*/
        }

        //internal bool IsIdentity {
        /*get {
            if (null != schemaTable.IsIdentity) { // MDAC 62336
                object value = dataRow[schemaTable.IsIdentity, DataRowVersion.Default];
                if (!Convert.IsDBNull(value)) {
                    return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                }
            }
            return false;
        }*/
        /*set {
            Debug.Assert(null != schemaTable.IsIdentity, "missing column IsIdentity");
            dataRow[schemaTable.IsIdentity] = value;
        }*/
        //}

        internal bool IsHidden
        {
            get
            {
                if (null != this.schemaTable.IsHidden)
                { // MDAC 62336
                    object value = this.dataRow[this.schemaTable.IsHidden, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return false;
            }
            /*set {
                Debug.Assert(null != schemaTable.IsHidden, "missing column IsHidden");
                dataRow[schemaTable.IsHidden] = value;
            }*/
        }

        internal bool IsLong
        {
            get
            {
                if (null != this.schemaTable.IsLong)
                { // MDAC 62336
                    object value = this.dataRow[this.schemaTable.IsLong, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return false;
            }
            /*set {
                Debug.Assert(null != schemaTable.IsLong, "missing column IsHidden");
                dataRow[schemaTable.IsLong] = value;
            }*/
        }

        internal bool IsReadOnly
        {
            get
            {
                if (null != this.schemaTable.IsReadOnly)
                { // MDAC 62336
                    object value = this.dataRow[this.schemaTable.IsReadOnly, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return false;
            }
            /*set {
                Debug.Assert(null != schemaTable.IsReadOnly, "missing column IsReadOnly");
                dataRow[schemaTable.IsReadOnly] = value;
            }*/
        }

        internal System.Type DataType
        {
            get
            {
                if (null != this.schemaTable.DataType)
                {
                    object value = this.dataRow[this.schemaTable.DataType, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return (System.Type)value;
                    }
                }
                return null;
            }
            /*set {
                Debug.Assert(null != schemaTable.DataType, "missing column DataType");
                dataRow[schemaTable.DataType] = value;
            }*/
        }

        internal bool AllowDBNull
        {
            get
            {
                if (null != this.schemaTable.AllowDBNull)
                {
                    object value = this.dataRow[this.schemaTable.AllowDBNull, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value))
                    {
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                    }
                }
                return true;
            }
            /*set {
                Debug.Assert(null != schemaTable.AllowDBNull, "missing column MaybeNull");
                dataRow[schemaTable.AllowDBNull] = value;
            }*/
        }

        /*internal Int32 ProviderType {
            get {
                if (null != schemaTable.ProviderType) {
                    object value = dataRow[schemaTable.ProviderType, DataRowVersion.Default];
                    if (!Convert.IsDBNull(value)) {
                        return Convert.ToInt32(value);
                    }
                }
                return 0;
            }
            set {
                Debug.Assert(null != schemaTable.ProviderType, "missing column ProviderType");
                dataRow[schemaTable.ProviderType] = value;
            }
        }*/

        internal Int32 UnsortedIndex
        {
            get
            {
                return (Int32)this.dataRow[this.schemaTable.UnsortedIndex, DataRowVersion.Default];
            }
        }
    }
}
