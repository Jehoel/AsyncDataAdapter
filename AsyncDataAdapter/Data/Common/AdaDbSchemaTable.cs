using System.Data;
using System.Data.Common;

namespace AsyncDataAdapter.Internal
{
    public sealed class AdaDbSchemaTable
    {
        private enum ColumnEnum
        {
            ColumnName,
            ColumnOrdinal,
            ColumnSize,
            BaseServerName,
            BaseCatalogName,
            BaseColumnName,
            BaseSchemaName,
            BaseTableName,
            IsAutoIncrement,
            IsUnique,
            IsKey,
            IsRowVersion,
            DataType,
            ProviderSpecificDataType,
            AllowDBNull,
            ProviderType,
            IsExpression,
            IsHidden,
            IsLong,
            IsReadOnly,
            SchemaMappingUnsortedIndex,
        }

        private static readonly string[] DBCOLUMN_NAME = new string[] {
            SchemaTableColumn        .ColumnName,
            SchemaTableColumn        .ColumnOrdinal,
            SchemaTableColumn        .ColumnSize,
            SchemaTableOptionalColumn.BaseServerName,
            SchemaTableOptionalColumn.BaseCatalogName,
            SchemaTableColumn        .BaseColumnName,
            SchemaTableColumn        .BaseSchemaName,
            SchemaTableColumn        .BaseTableName,
            SchemaTableOptionalColumn.IsAutoIncrement,
            SchemaTableColumn        .IsUnique,
            SchemaTableColumn        .IsKey,
            SchemaTableOptionalColumn.IsRowVersion,
            SchemaTableColumn        .DataType,
            SchemaTableOptionalColumn.ProviderSpecificDataType,
            SchemaTableColumn        .AllowDBNull,
            SchemaTableColumn        .ProviderType,
            SchemaTableColumn        .IsExpression,
            SchemaTableOptionalColumn.IsHidden,
            SchemaTableColumn        .IsLong,
            SchemaTableOptionalColumn.IsReadOnly,
            AdaDbSchemaRow           .SchemaMappingUnsortedIndex,
        };

#pragma warning disable IDE0052 // Remove unread private members
        private readonly DataTable            dataTable;
#pragma warning restore IDE0052
        private readonly DataColumnCollection columns;
        private readonly DataColumn[]         columnCache = new DataColumn[DBCOLUMN_NAME.Length];
        private readonly bool                 returnProviderSpecificTypes;

        internal AdaDbSchemaTable(DataTable dataTable, bool returnProviderSpecificTypes)
        {
            this.dataTable = dataTable;
            this.columns   = dataTable.Columns;
            this.returnProviderSpecificTypes = returnProviderSpecificTypes;
        }

        internal DataColumn ColumnName      { get { return this.CachedDataColumn(ColumnEnum.ColumnName); } }
        internal DataColumn Size            { get { return this.CachedDataColumn(ColumnEnum.ColumnSize); } }
        internal DataColumn BaseServerName  { get { return this.CachedDataColumn(ColumnEnum.BaseServerName); } }
        internal DataColumn BaseColumnName  { get { return this.CachedDataColumn(ColumnEnum.BaseColumnName); } }
        internal DataColumn BaseTableName   { get { return this.CachedDataColumn(ColumnEnum.BaseTableName); } }
        internal DataColumn BaseCatalogName { get { return this.CachedDataColumn(ColumnEnum.BaseCatalogName); } }
        internal DataColumn BaseSchemaName  { get { return this.CachedDataColumn(ColumnEnum.BaseSchemaName); } }
        internal DataColumn IsAutoIncrement { get { return this.CachedDataColumn(ColumnEnum.IsAutoIncrement); } }
        internal DataColumn IsUnique        { get { return this.CachedDataColumn(ColumnEnum.IsUnique); } }
        internal DataColumn IsKey           { get { return this.CachedDataColumn(ColumnEnum.IsKey); } }
        internal DataColumn IsRowVersion    { get { return this.CachedDataColumn(ColumnEnum.IsRowVersion); } }
        internal DataColumn AllowDBNull     { get { return this.CachedDataColumn(ColumnEnum.AllowDBNull); } }
        internal DataColumn IsExpression    { get { return this.CachedDataColumn(ColumnEnum.IsExpression); } }
        internal DataColumn IsHidden        { get { return this.CachedDataColumn(ColumnEnum.IsHidden); } }
        internal DataColumn IsLong          { get { return this.CachedDataColumn(ColumnEnum.IsLong); } }
        internal DataColumn IsReadOnly      { get { return this.CachedDataColumn(ColumnEnum.IsReadOnly); } }
        internal DataColumn UnsortedIndex   { get { return this.CachedDataColumn(ColumnEnum.SchemaMappingUnsortedIndex); } }

        internal DataColumn DataType
        {
            get
            {
                if (this.returnProviderSpecificTypes)
                {
                    return this.CachedDataColumn(ColumnEnum.ProviderSpecificDataType, ColumnEnum.DataType);
                }
                return this.CachedDataColumn(ColumnEnum.DataType);
            }
        }

        private DataColumn CachedDataColumn(ColumnEnum column)
        {
            return this.CachedDataColumn(column, column);
        }

        private DataColumn CachedDataColumn(ColumnEnum column, ColumnEnum column2)
        {
            DataColumn dataColumn = this.columnCache[(int)column];
            if (null == dataColumn)
            {
                int index = this.columns.IndexOf(DBCOLUMN_NAME[(int)column]);
                if ((-1 == index) && (column != column2))
                {
                    index = this.columns.IndexOf(DBCOLUMN_NAME[(int)column2]);
                }
                if (-1 != index)
                {
                    dataColumn = this.columns[index];
                    this.columnCache[(int)column] = dataColumn;
                }
            }
            return dataColumn;
        }
    }
}
