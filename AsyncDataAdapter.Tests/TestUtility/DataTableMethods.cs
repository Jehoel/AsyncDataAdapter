using System;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Text;

using Shouldly;
using System.Text.RegularExpressions;

namespace AsyncDataAdapter.Tests
{
    public static class DataTableMethods
    {
        /// <summary>Returns the number of rows that were modified in each table (keyed by <see cref="DataTable.TableName"/>).</summary>
        public static Dictionary<String,Int32> MutateDataSet( DataSet dataSet )
        {
            // Do the exact same mutation in both DataSets: a diagonal line of nulls, methinks.

            Dictionary<String,Int32> counts = new Dictionary<string, int>();

            foreach( DataTable table in dataSet.Tables )
            {
                Int32 rows = Math.Min( 30, table.Rows   .Count );
                Int32 cols = Math.Min( 30, table.Columns.Count );
                Int32 max  = Math.Min( rows, cols );

                for( Int32 i = 1; i < max; i++ ) // Don't modify column 0, that's the PK column.
                {
                    DataRow row = table.Rows[i];

                    Boolean rowIsModified = false;

                    if( !row.IsNull( i ) )
                    {
                        row[ i ] = DBNull.Value;
                        rowIsModified = true;
                    }

                    if( rowIsModified )
                    {
                        Int32 current = counts.TryGetValue( table.TableName, out Int32 count ) ? count : 0;
                        counts[ table.TableName ] = current + 1;
                    }
                }
            }

            return counts;
        }

        public static Boolean DataSetEquals( DataSet left, DataSet right )
        {
            _ = left .ShouldNotBeNull();
            _ = right.ShouldNotBeNull();
            Object.ReferenceEquals( left, right ).ShouldBeFalse();

            //

            left.Tables.Count.ShouldBe( right.Tables.Count );

            for( Int32 t = 0; t < left.Tables.Count; t++ )
            {
                DataTable lt = left .Tables[t];
                DataTable rt = right.Tables[t];

                Boolean eq = DataTableEquals( lt, rt );
                if( !eq ) return false;
            }

            return true;
        }

        public static Boolean DataTablesEquals( DataTable[] left, DataTable[] right )
        {
            _ = left .ShouldNotBeNull();
            _ = right.ShouldNotBeNull();

            left.Length.ShouldBe( right.Length );

            for( Int32 i = 0; i < left.Length; i++ )
            {
                Boolean teq = DataTableEquals( left[i], right[i] );
                if( !teq ) return false;
            }

            return true;
        }

        public static Boolean DataTableEquals( DataTable left, DataTable right )
        {
            _ = left .ShouldNotBeNull();
            _ = right.ShouldNotBeNull();
            Object.ReferenceEquals( left, right ).ShouldBeFalse(); // Ensure the tables are not the same object in memory.

            //

            left.Namespace.ShouldBe( right.Namespace );
            left.TableName.ShouldBe( right.TableName );

            left.Rows   .Count.ShouldBe( right.Rows   .Count );
            left.Columns.Count.ShouldBe( right.Columns.Count );

            for( Int32 y = 0; y < left.Rows.Count; y++ )
            {
                DataRow leftRow  = left .Rows[y];
                DataRow rightRow = right.Rows[y];

                for( Int32 x = 0; x < left.Columns.Count; x++ )
                {
                    Object leftValue  = leftRow [x];
                    Object rightValue = rightRow[x];

                    Boolean eq = CellsEquals( leftValue, rightValue );
                    if( !eq ) return false;
                }
            }

            return true;
        }

        public static Boolean CellsEquals( Object leftValue, Object rightValue )
        {
            if( leftValue is null && rightValue is null ) return true;
            if( leftValue is null || rightValue is null ) return false;

            Type leftType = leftValue .GetType();
            Type righType = rightValue.GetType();

            if( leftType.Equals( righType ) )
            {
                return leftValue.Equals( rightValue );
            }

            return false;
        }

        private static readonly Regex _updateStatement = new Regex( @"^UPDATE\s+(?<tableName>.*?)\s+", RegexOptions.Compiled );

        public static Int32 GetNonQueryResultRowCountValue( FakeDb.FakeDbCommand cmd, Dictionary<String,Int32> rowsModified )
        {
            // Special-case for UpdateCommands from DbDataAdapter and DbCommandBuilder:
            // If the query looks like this:
            /*

            UPDATE [Table_1] SET [PK] = @p1, [Col1] = @p2, [Col2] = @p3, [Col3] = @p4, [Col4] = @p5, [Col5] = @p6, [Col6] = @p7, [Col7] = @p8, [Col8] = @p9, [Col9] = @p10, [Col10] = @p11, [Col11] = @p12, [Col12] = @p13, [Col13] = @p14, [Col14] = @p15, [Col15] = @p16, [Col16] = @p17, [Col17] = @p18, [Col18] = @p19, [Col19] = @p20, [Col20] = @p21, [Col21] = @p22, [Col22] = @p23 WHERE (([PK] = @p24) AND ((@p25 = 1 AND [Col1] IS NULL) OR ([Col1] = @p26)) AND ((@p27 = 1 AND [Col2] IS NULL) OR ([Col2] = @p28)) AND ((@p29 = 1 AND [Col3] IS NULL) OR ([Col3] = @p30)) AND ((@p31 = 1 AND [Col4] IS NULL) OR ([Col4] = @p32)) AND ((@p33 = 1 AND [Col5] IS NULL) OR ([Col5] = @p34)) AND ((@p35 = 1 AND [Col6] IS NULL) OR ([Col6] = @p36)) AND ((@p37 = 1 AND [Col7] IS NULL) OR ([Col7] = @p38)) AND ((@p39 = 1 AND [Col8] IS NULL) OR ([Col8] = @p40)) AND ((@p41 = 1 AND [Col9] IS NULL) OR ([Col9] = @p42)) AND ((@p43 = 1 AND [Col10] IS NULL) OR ([Col10] = @p44)) AND ((@p45 = 1 AND [Col11] IS NULL) OR ([Col11] = @p46)) AND ((@p47 = 1 AND [Col12] IS NULL) OR ([Col12] = @p48)) AND ((@p49 = 1 AND [Col13] IS NULL) OR ([Col13] = @p50)) AND ((@p51 = 1 AND [Col14] IS NULL) OR ([Col14] = @p52)) AND ((@p53 = 1 AND [Col15] IS NULL) OR ([Col15] = @p54)) AND ((@p55 = 1 AND [Col16] IS NULL) OR ([Col16] = @p56)) AND ((@p57 = 1 AND [Col17] IS NULL) OR ([Col17] = @p58)) AND ((@p59 = 1 AND [Col18] IS NULL) OR ([Col18] = @p60)) AND ((@p61 = 1 AND [Col19] IS NULL) OR ([Col19] = @p62)) AND ((@p63 = 1 AND [Col20] IS NULL) OR ([Col20] = @p64)) AND ((@p65 = 1 AND [Col21] IS NULL) OR ([Col21] = @p66)) AND ((@p67 = 1 AND [Col22] IS NULL) OR ([Col22] = @p68)))

            */

            String commandText = cmd.CommandText;

            Match updateStatementMatch = _updateStatement.Match( commandText );
            if( updateStatementMatch.Success )
            {
                String tableName = updateStatementMatch.Groups["tableName"].Value;
                tableName = tableName.Trim( '[', ']' );
                
                if( rowsModified.TryGetValue( tableName, out Int32 tableCountRowsModified ) )
                {
                    // By-design, DbDataAdapter executes an UPDATE query *for each row* (though it may batch them, they're still conceptually separate DbCommand instances)
                    // So this should always be 1...

                    return 1;
                }
            }

            //return 0;

            throw new InvalidOperationException( "Unexpected command." );
        }
    }
}
