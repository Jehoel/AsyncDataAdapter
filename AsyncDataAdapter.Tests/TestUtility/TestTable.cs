using System;
using System.Collections.Generic;
using System.Data;

namespace AsyncDataAdapter.Tests
{
    public class TestTable
    {
        public TestTable( int index, string name, string[] columnNames, Type[] columnTypes, List<Object[]> rows )
        {
            this.Index       = index;
            this.Name        = name        ?? throw new ArgumentNullException(nameof(name));
            this.ColumnNames = columnNames ?? throw new ArgumentNullException(nameof(columnNames));
            this.ColumnTypes = columnTypes ?? throw new ArgumentNullException(nameof(columnTypes));
            this.Rows        = rows        ?? throw new ArgumentNullException(nameof(rows));
        }

        public Int32 Index { get; }

        public String Name { get; } // hold on, do names exist outside of DataTable/DataSet?

        public String[] ColumnNames { get; }
        
        public Type[] ColumnTypes { get; }

        public List<Object[]> Rows { get; }

        public Int32 PKColumnIndex => 0; // The 0th column is always the PK column.

        //

        /// <summary>Creates a DataTable with the same structure as this table, but without any rows. This table can be used by <see cref="System.Data.DataTableReader"/> for use with <see cref="System.Data.Common.DbDataReader.GetSchemaTable"/>.</summary>
        public DataTable CreateMinimalSchemaTable()
        {
            DataTable dt = new DataTable( tableName: this.Name );

            for( Int32 x = 0; x < this.ColumnNames.Length; x++ )
            {
                DataColumn col = new DataColumn( columnName: this.ColumnNames[x], dataType: this.ColumnTypes[x] );
                if( x == 0 )
                {
                    col.Unique      = true;
                    col.AllowDBNull = false;
                }

                dt.Columns.Add( col );
            }

            dt.PrimaryKey = new DataColumn[] { dt.Columns[0] };

            return dt;
        }
    }
}
