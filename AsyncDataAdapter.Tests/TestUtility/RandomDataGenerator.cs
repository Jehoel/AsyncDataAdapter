using System;
using System.Collections.Generic;
using System.Globalization;

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
    }

    public static class RandomDataGenerator
    {
        public static List<TestTable> CreateRandomTables( Int32 seed, Int32 tableCount, params Int32[] allowZeroRowsInTablesByIdx )
        {
            Random rng = new Random( Seed: seed ); // Use a const seed so the tests are deterministic and reproducible.

            List<TestTable> tables = new List<TestTable>( capacity: tableCount );

            HashSet<Int32> allowZeroRowsIn = new HashSet<Int32>( collection: allowZeroRowsInTablesByIdx ?? Array.Empty<Int32>() );

            for( Int32 i = 0; i < tableCount; i++ )
            {
                TestTable table = CreateRandomTable( i, allowZeroRows: allowZeroRowsIn.Contains( i ), rng );
                tables.Add( table );
            }

            return tables;
        }

        public static TestTable CreateRandomTable( Int32 idx, Boolean allowZeroRows, Random rng )
        {
            Int32 rows;
            if( allowZeroRows )
            {
                rows = rng.Next( minValue: 0, maxValue: 6  ); // Fudging the odds to ensure a higher probability of zero-rows.
            }
            else
            {
                rows = rng.Next( minValue: 1, maxValue: 101 ); // maxValue is exclusive. 
            }

            Int32 cols = rng.Next( minValue: 2, maxValue:  26 );
           
            //

            String[] colNames = new String[ cols ];
            for( Int32 x = 0; x < cols; x++ )
            {
                colNames[x] = String.Format( CultureInfo.InvariantCulture, "Col{0}", x );
            }

            Type[] colTypes = new Type[ cols ];
            for( Int32 x = 0; x < cols; x++ )
            {
                colTypes[x] = GetRandomColumnType( rng );
            }
            
            //

            List<Object[]> rowsList = new List<Object[]>( capacity: rows );

            for( Int32 y = 0; y < rows; y++ )
            {
                Object[] row = new Object[ cols ];

                for( Int32 x = 0; x < cols; x++ )
                {
                    row[x] = GetRandomValue( rng, colTypes[x] );
                }

                rowsList.Add( row );
            }

            return new TestTable(
                index      : idx,
                name       : String.Format( CultureInfo.InvariantCulture, "Table_{0}", idx + 1 ),
                columnNames: colNames,
                columnTypes: colTypes,
                rows       : rowsList
            );
        }

        private static readonly Type[] _types = new Type[]
        {
            typeof(Boolean),
            typeof(Byte),
            typeof(Int16),
            typeof(Int32),
            typeof(Int64),
            typeof(String),
            typeof(DateTime),
            typeof(DateTimeOffset),
            typeof(Single),
            typeof(Double),
            typeof(Decimal),
            typeof(Byte[]),
            typeof(Guid)
        };

        private static Type GetRandomColumnType( Random rng )
        {
            return _types[ rng.Next( minValue: 0, maxValue: _types.Length ) ];
        }

        private static readonly String _alphabet = @"0123456789ABCDEFGHIJKLMNOPQRSTUVWYZ";

        private static Object GetRandomValue( Random rng, Type type )
        {
            Boolean isNull = rng.Next( minValue: 0, maxValue: 10 ) == 0;
            if( isNull ) return DBNull.Value;

            if( type == typeof(Boolean) )
            {
                return rng.Next( minValue: 0, maxValue: 2 ) == 0;
            }
            else if( type == typeof(Byte) )
            {
                Int32 value = rng.Next( minValue: 0, maxValue: Byte.MaxValue + 1 );
                return (Byte)value;
            }
            else if( type == typeof(Int16) )
            {
                Int32 value = rng.Next( minValue: 0, maxValue: Int16.MaxValue + 1 );
                return (Int16)value;
            }
            else if( type == typeof(Int32) )
            {
                Int32 value = rng.Next( minValue: 0, maxValue: Int32.MaxValue );
                return value;
            }
            else if( type == typeof(Int64) )
            {
                Byte[] i64Bytes = new Byte[8];
                rng.NextBytes( i64Bytes );
                return BitConverter.ToInt64( i64Bytes );
            }
            else if( type == typeof(String) )
            {
                // 10% chance of zero-length string:
                if( rng.Next( minValue: 0, maxValue: 10 ) == 0 )
                {
                    return String.Empty;
                }

                Int32 length = rng.Next( minValue: 1, maxValue: 255 );
                Char[] chars = new Char[ length ];
                for( Int32 i = 0; i < chars.Length; i++ )
                {
                    Int32 randomCharIdx = rng.Next( minValue: 0, maxValue: _alphabet.Length ); // Note that a zero `Next()` value is the index into the alphabet string, and NOT a null '\0' character. I don't want to test that .
                    chars[i] = _alphabet[ randomCharIdx ];
                }

                String toString = new String( chars );
                return toString;
            }
            else if( type == typeof(DateTime) )
            {
                Int32 randomUnixTime = rng.Next( minValue: 0, maxValue: Int32.MaxValue ); // Year 2038. I assume a legit DbDataAdapter with Async support will exist by then...
                return DateTimeOffset.FromUnixTimeSeconds( randomUnixTime ).UtcDateTime;
            }
            else if( type == typeof(DateTimeOffset) )
            {
                Int32 randomUnixTime = rng.Next( minValue: 0, maxValue: Int32.MaxValue ); // Year 2038. I assume a legit DbDataAdapter with Async support will exist by then...
                return DateTimeOffset.FromUnixTimeSeconds( randomUnixTime );
            }
            else if( type == typeof(Single) )
            {
                Double dbl = rng.NextDouble() * rng.Next( minValue: 0, maxValue: Int32.MaxValue );
                return (Single)dbl;
            }
            else if( type == typeof(Double) )
            {
                Double dbl = rng.NextDouble() * rng.Next( minValue: 0, maxValue: Int32.MaxValue );
                return dbl;
            }
            else if( type == typeof(Decimal) )
            {
                Double dbl = rng.NextDouble() * rng.Next( minValue: 0, maxValue: Int32.MaxValue );
                return new Decimal( dbl );
            }
            else if( type == typeof(Byte[]) )
            {
                // 10% chance of zero-length bytes:
                if( rng.Next( minValue: 0, maxValue: 10 ) == 0 )
                {
                    return Array.Empty<Byte>();
                }

                Int32 length = rng.Next( minValue: 1, maxValue: 65535 );
                Byte[] bytes = new Byte[ length ];
                for( Int32 i = 0; i < bytes.Length; i++ )
                {
                    Int32 randomByteValue = rng.Next( minValue: 0, maxValue: 256 ); // Allow NULL bytes?
                    bytes[i] = (Byte)randomByteValue;
                }

                return bytes;
            }
            else if( type == typeof(Guid) )
            {
                return Guid.NewGuid();
            }
            else
            {
                throw new ArgumentException( "Unsupported type." );
            }
        }
    }
}
