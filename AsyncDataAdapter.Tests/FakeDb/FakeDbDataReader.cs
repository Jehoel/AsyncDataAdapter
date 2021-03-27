using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    public class FakeDbDataReader : DbDataReader
    {
        public FakeDbDataReader( FakeDbCommand cmd )
        {
            this.Command = cmd ?? throw new ArgumentNullException(nameof(cmd));
        }

        public AsyncMode AsyncMode { get; set; }

        #region TestTables

        public FakeDbCommand Command { get; }

        public Int32 CurrentTableIdx { get; set; } =  0;
        public Int32 CurrentRowIdx   { get; set; } = -1;

        /// <summary>Lists all source tables in the current fake source result.</summary>
        public List<TestTable> AllTables { get; set; } = new List<TestTable>();


        public TestTable CurrentTable
        {
            get
            {
                if( this.CurrentTableIdx < this.AllTables.Count )
                {
                    return this.AllTables[ this.CurrentTableIdx ];
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>Lists all source rows in the current table. Returns null if there is no current row.</summary>
        public IList<Object[]> Rows
        {
            get
            {
                TestTable currentTable = this.CurrentTable;
                if( currentTable is null ) return null;

                return currentTable.Rows;
            }
        }

        /// <summary>Lists all columns (fields) in the current row. Returns <see langword="null"/> if no row is available.</summary>
        public Object[] RowData
        {
            get
            {
                IList<Object[]> currentTableRows = this.Rows;
                if( currentTableRows is null || this.CurrentRowIdx < 0 || this.CurrentRowIdx >= currentTableRows.Count )
                {
                    return null;
                }
                else
                {
                    return currentTableRows[ this.CurrentRowIdx ];
                }
            }
            set
            {
                if( value is null ) throw new ArgumentNullException(nameof(value));

                IList<Object[]> currentTableRows = this.Rows;
                if( currentTableRows is null || this.CurrentRowIdx < 0 || this.CurrentRowIdx >= currentTableRows.Count )
                {
                    throw new InvalidOperationException( "Cannot set the current row when no results are loaded." );
                }

                if( value.Length != currentTableRows.Count )
                {
                    String msg = String.Format( CultureInfo.CurrentCulture, "Cannot set the current row using an array with {0} columns when the current table has {1} columns.", value.Length, currentTableRows.Count );
                    throw new InvalidOperationException( msg );
                }

                // TODO: Check column types too?

                //

                this.CurrentTable.Rows[ this.CurrentRowIdx ] = value;
            }
        }

        /// <summary>Column names.</summary>
        public String[] Names   { get; set; }

        /// <summary>Column types. Don't use <see cref="Nullable{T}"/> for nullable columns - all columns can store null values (as <see cref="DBNull"/>).</summary>
        public Type[]   Types   { get; set; }

        #endregion

        public void ResetAndLoadTestData( List<TestTable> tables )
        {
            if (tables is null) throw new ArgumentNullException(nameof(tables));

            //

            this.AllTables = tables;

            if( tables.Count == 0 )
            {
                this.CurrentTableIdx = -1;
                this.CurrentRowIdx   = -1;
            }
            else
            {
                this.CurrentTableIdx =  0; // DataReaders are always positioned at the start of the first table when they have results.
                this.CurrentRowIdx   = -1;
            }
        }

        #region Get typed values

        public override bool GetBoolean(int ordinal)
        {
            return (bool)this.RowData[ordinal];
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)this.RowData[ordinal];
        }

        public override char GetChar(int ordinal)
        {
            return (char)this.RowData[ordinal];
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)this.RowData[ordinal];
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)this.RowData[ordinal];
        }

        public override double GetDouble(int ordinal)
        {
            return (double)this.RowData[ordinal];
        }

        public override float GetFloat(int ordinal)
        {
            return (float)this.RowData[ordinal];
        }

        public override Guid GetGuid(int ordinal)
        {
            return (Guid)this.RowData[ordinal];
        }

        public override short GetInt16(int ordinal)
        {
            return (short)this.RowData[ordinal];
        }

        public override int GetInt32(int ordinal)
        {
            return (int)this.RowData[ordinal];
        }

        public override long GetInt64(int ordinal)
        {
            return (long)this.RowData[ordinal];
        }

        public override string GetString(int ordinal)
        {
            return (string)this.RowData[ordinal];
        }

        public override object GetValue(int ordinal)
        {
            return (bool)this.RowData[ordinal];
        }

        #endregion

        #region More reader methods

        public override int GetValues(object[] values)
        {
            Int32 length = Math.Min( values.Length, this.RowData.Length );
            Array.Copy( this.RowData, destinationArray: values, length: length );
            return length;
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            Byte[] bytes = (Byte[])this.RowData[ordinal];

            Int32 minLength = Math.Min( length, bytes.Length );
            Array.Copy( sourceArray: bytes, sourceIndex: dataOffset, destinationArray: buffer, destinationIndex: bufferOffset, length: minLength );
            return minLength;
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            char[] bytes = (char[])this.RowData[ordinal];

            Int32 minLength = Math.Min( length, bytes.Length );
            Array.Copy( sourceArray: bytes, sourceIndex: dataOffset, destinationArray: buffer, destinationIndex: bufferOffset, length: minLength );
            return minLength;
        }

        public override string GetDataTypeName(int ordinal)
        {
            return this.Types[ordinal].Name;
        }

        public override IEnumerator GetEnumerator()
        {
            return this.Rows.GetEnumerator();
        }

        public override Type GetFieldType(int ordinal)
        {
            return this.RowData[ordinal].GetType();
        }

        public override string GetName(int ordinal)
        {
            return this.Names[ordinal];
        }

        public override int GetOrdinal(string name)
        {
            return Array.IndexOf( this.Names, name );
        }

        public override bool IsDBNull(int ordinal)
        {
            return Object.ReferenceEquals( this.RowData[ordinal], DBNull.Value );
        }

        public override int Depth => 1;

        public override int FieldCount => this.RowData.Length;

        public override bool HasRows => this.RowData != null;

        public override bool IsClosed => this.IsClosed2;

        public Boolean IsClosed2 { get; set; }

        public override object this[int ordinal] => this.RowData[ordinal];

        public override object this[string name] => this.RowData[ this.GetOrdinal( name ) ];

        public override int RecordsAffected => this.RecordsAffected2;
        
        public int RecordsAffected2 { get; set; }

        #endregion

        //

        private Boolean NextResultImpl()
        {
            Int32 maxIdx = this.AllTables.Count - 1;
            if( this.CurrentTableIdx < maxIdx )
            {
                this.CurrentTableIdx++;
                this.CurrentRowIdx = -1;
                return true;
            }
            else // implicit: this.CurrentTableIdx >= maxIdx )
            {
                return false;
            }
        }

        public override Boolean NextResult()
        {
            if( this.AsyncMode.AllowOld() )
            {
                return this.NextResultImpl();
            }
            else
            {
                throw new InvalidOperationException( "Synchronous methods are disabled." );
            }
        }

        public override async Task<Boolean> NextResultAsync(CancellationToken cancellationToken)
        {
            if( this.AsyncMode.HasFlag( AsyncMode.AwaitAsync ) )
            {
                await Task.Delay( 100 ).ConfigureAwait(false);

                return this.NextResultImpl();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                return this.NextResultImpl();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                return await base.NextResultAsync();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                return await Task.Run( () => this.NextResultImpl() );
            }
            else
            {
                throw new InvalidOperationException( "Asynchronous methods are disabled." );
            }
        }

        //

        private Boolean ReadImpl()
        {
            Int32 maxIdx = this.Rows.Count - 1;
            if( this.CurrentRowIdx < maxIdx)
            {
                this.CurrentRowIdx++;
                return true;
            }
            else
            {
                return false;
            }
        }

        public override Boolean Read()
        {
            if( this.AsyncMode.AllowOld() )
            {
                return this.ReadImpl();
            }
            else
            {
                throw new InvalidOperationException( "Synchronous methods are disabled." );
            }
        }

        public override async Task<Boolean> ReadAsync( CancellationToken cancellationToken )
        {
            if( this.AsyncMode.HasFlag( AsyncMode.AwaitAsync ) )
            {
                await Task.Delay( 1 ).ConfigureAwait(false);

                return this.ReadImpl();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 1 );

                return this.ReadImpl();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                return await base.ReadAsync();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                return await Task.Run( () => this.ReadImpl() );
            }
            else
            {
                throw new InvalidOperationException( "Asynchronous methods are disabled." );
            }
        }
    }
}
