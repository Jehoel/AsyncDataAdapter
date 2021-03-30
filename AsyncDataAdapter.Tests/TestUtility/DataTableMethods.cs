using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;

using AsyncDataAdapter.Internal;

using Shouldly;

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
                MutateDataTable( table, counts );
            }

            return counts;
        }

        /// <summary>Returns the number of rows that were modified in each table (keyed by <see cref="DataTable.TableName"/>).</summary>
        public static Dictionary<String,Int32> MutateDataTable( DataTable table )
        {
            Dictionary<String,Int32> counts = new Dictionary<string, int>();

            MutateDataTable( table, counts );

            return counts;
        }

        private static void MutateDataTable( DataTable table, Dictionary<String,Int32> counts )
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

        public static Boolean DataSetEquals( DataSet left, DataSet right, out String differences )
        {
            _ = left .ShouldNotBeNull();
            _ = right.ShouldNotBeNull();
            Object.ReferenceEquals( left, right ).ShouldBeFalse();

            //

            left.Tables.Count.ShouldBe( right.Tables.Count );

            for( Int32 tableIdx = 0; tableIdx < left.Tables.Count; tableIdx++ )
            {
                DataTable leftTable  = left .Tables[tableIdx];
                DataTable rightTable = right.Tables[tableIdx];

                Boolean tablesSame = DataTableEquals( leftTable, rightTable, out Int32? diffRowIdx, out Int32? diffColIdx );
                if( !tablesSame )
                {
                    Object leftValue  = leftTable .Rows[diffRowIdx.Value][diffColIdx.Value];
                    Object rightValue = rightTable.Rows[diffRowIdx.Value][diffColIdx.Value];

                    differences = String.Format( CultureInfo.CurrentCulture, "First difference found in table {0:D}, at row {1:D} and column {2:D} ( {3} vs {4} )", tableIdx, diffRowIdx, diffColIdx, leftValue, rightValue );
                    return false;
                }
            }

            differences = null;
            return true;
        }

        public static Boolean DataTablesEquals( DataTable[] left, DataTable[] right, out String differences )
        {
            _ = left .ShouldNotBeNull();
            _ = right.ShouldNotBeNull();

            left.Length.ShouldBe( right.Length );

            for( Int32 tableIdx = 0; tableIdx < left.Length; tableIdx++ )
            {
                
            }

            differences = null;
            return true;
        }

        public static Boolean DataTableEquals( DataTable leftTable, DataTable rightTable, Object tableId, out String differences )
        {
            Boolean tablesSame = DataTableEquals( leftTable, rightTable, out Int32? diffRowIdx, out Int32? diffColIdx );
            if( !tablesSame )
            {
                Object leftValue  = leftTable .Rows[diffRowIdx.Value][diffColIdx.Value];
                Object rightValue = rightTable.Rows[diffRowIdx.Value][diffColIdx.Value];

                differences = String.Format( CultureInfo.CurrentCulture, "First difference found in table {0:D}, at row {1:D} and column {2:D} ( {3} vs {4} )", tableId, diffRowIdx, diffColIdx, leftValue, rightValue );
                return false;
            }

            differences = null;
            return true;
        }

        public static Boolean DataTableEquals( DataTable left, DataTable right, out Int32? diffRowIdx, out Int32? diffColIdx )
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
                    if( !eq )
                    {
                        diffRowIdx = y;
                        diffColIdx = x;
                        return false;
                    }
                }
            }

            diffRowIdx = null;
            diffColIdx = null;
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
                if( leftType == typeof(Byte[]) )
                {
                    Byte[] leftBytes  = (Byte[])leftValue;
                    Byte[] rightBytes = (Byte[])rightValue;

                    return System.Linq.Enumerable.SequenceEqual( leftBytes, rightBytes );
                }
                else
                {
                    return leftValue.Equals( rightValue );
                }
            }

            return false;
        }

        public static Int32 GetUpdateStatementNonQueryResultRowCountValue( String expectedTableName, DbDataAdapter adapter, DataTables data, FakeDb.FakeDbCommand cmd, Dictionary<String,Int32> rowsModified, List<(String tableName, String command)> log = null )
        {
            // Special-case for UpdateCommands from DbDataAdapter and DbCommandBuilder:
            // If the query looks like this:
            /*

            UPDATE
                [Table_1]
            SET
                [PK] = @p1,
                [Col1] = @p2,
                [Col2] = @p3,
                [Col3] = @p4,
                [Col4] = @p5,
                [Col5] = @p6,
                [Col6] = @p7, [Col7] = @p8, [Col8] = @p9, [Col9] = @p10, [Col10] = @p11, [Col11] = @p12, [Col12] = @p13, [Col13] = @p14, [Col14] = @p15, [Col15] = @p16,
                [Col16] = @p17, [Col17] = @p18, [Col18] = @p19, [Col19] = @p20,
                [Col20] = @p21,
                [Col21] = @p22,
                [Col22] = @p23
            WHERE
                (
                    ([PK] = @p24)
                    AND
                    (
                        (@p25 = 1 AND [Col1] IS NULL)
                        OR
                        ([Col1] = @p26)
                    )
                    AND
                    (
                        (@p27 = 1 AND [Col2] IS NULL)
                        OR
                        ([Col2] = @p28)
                    )
                    AND
                    (
                        (@p29 = 1 AND [Col3] IS NULL)
                        OR
                        ([Col3] = @p30)
                    )
                    AND
                    (
                        (@p31 = 1 AND [Col4] IS NULL)
                        OR
                        ([Col4] = @p32)
                    )
                    AND
                    (
                        (@p33 = 1 AND [Col5] IS NULL)
                        OR
                        ([Col5] = @p34)
                    )
                    AND ((@p35 = 1 AND [Col6] IS NULL) OR ([Col6] = @p36)) AND ((@p37 = 1 AND [Col7] IS NULL) OR ([Col7] = @p38)) AND ((@p39 = 1 AND [Col8] IS NULL) OR ([Col8] = @p40)) AND ((@p41 = 1 AND [Col9] IS NULL) OR ([Col9] = @p42)) AND ((@p43 = 1 AND [Col10] IS NULL) OR ([Col10] = @p44)) AND ((@p45 = 1 AND [Col11] IS NULL) OR ([Col11] = @p46)) AND ((@p47 = 1 AND [Col12] IS NULL) OR ([Col12] = @p48)) AND ((@p49 = 1 AND [Col13] IS NULL) OR ([Col13] = @p50)) AND ((@p51 = 1 AND [Col14] IS NULL) OR ([Col14] = @p52)) AND ((@p53 = 1 AND [Col15] IS NULL) OR ([Col15] = @p54)) AND ((@p55 = 1 AND [Col16] IS NULL) OR ([Col16] = @p56)) AND ((@p57 = 1 AND [Col17] IS NULL) OR ([Col17] = @p58)) AND ((@p59 = 1 AND [Col18] IS NULL) OR ([Col18] = @p60)) AND ((@p61 = 1 AND [Col19] IS NULL) OR ([Col19] = @p62)) AND ((@p63 = 1 AND [Col20] IS NULL) OR ([Col20] = @p64)) AND ((@p65 = 1 AND [Col21] IS NULL) OR ([Col21] = @p66)) AND ((@p67 = 1 AND [Col22] IS NULL) OR ([Col22] = @p68))
                )

            */

            String commandText = cmd.CommandText;

            if( IsUpdateStatement( commandText, out String tableName, out String predicate ) )
            {
                log?.Add( ( tableName, commandText ) );

                // By-design, DbDataAdapter executes an UPDATE query *for each row* (though it may batch them, they're still conceptually separate DbCommand instances)
                // So this should always be 1...

                if( expectedTableName != tableName )
                {
                    String msg = String.Format( CultureInfo.CurrentCulture, "Expected the DbDataAdapter.UpdateCommand to UPDATE the {0} table, but encountered table name {1} instead.", expectedTableName, tableName );
                    throw new InvalidOperationException( msg );
                }

                // If the query has "[PK] = " in its predicate then it's only going to update a single row.
                if( predicate.IndexOf( "[PK] = ", StringComparison.Ordinal ) > -1 )
                {
                    return 1;
                }
                else
                {
                    throw new InvalidOperationException( "Expected the DbDataAdapter.UpdateCommand to target a specific row by PK, but the PK predicate was not found in the WHERE clause." );
                }
            }
            else
            {
                log?.Add( ( "(Unknown)", commandText ) );
            }

            //

            if( commandText != null )
            {
                String summary = commandText.Length >= 100 ? ( commandText.Substring( startIndex: 0, length: 100 ) + "..." ) : commandText;

                throw new InvalidOperationException( "Unexpected command: " + summary );
            }
            else
            {
                throw new InvalidOperationException( "Unexpected command: CommandText is null." );
            }
        }

        private static readonly Regex _updateStatement = new Regex( @"^UPDATE\s+(?<tableName>.*?)\s+", RegexOptions.Compiled );
        private static readonly Regex _whereClause     = new Regex( @"\bWHERE\b+(?<predicate>.*)$", RegexOptions.Compiled );

        private static Boolean IsUpdateStatement( String commandText, out String tableName, out String predicate )
        {
            Match updateStatementMatch = _updateStatement.Match( commandText );
            if( updateStatementMatch.Success )
            {
                tableName = updateStatementMatch.Groups["tableName"].Value;
                tableName = tableName.Trim( '[', ']' );

                //

                Match whereClauseMatch = _whereClause.Match( commandText );
                if( whereClauseMatch.Success )
                {
                    predicate = whereClauseMatch.Groups["predicate"].Value;
                }
                else
                {
                    predicate = null;
                }

                return true;
            }

            tableName = null;
            predicate = null;
            return false;
        }
    }
}
