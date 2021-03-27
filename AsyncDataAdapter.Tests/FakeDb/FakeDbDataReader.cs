using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
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

        public FakeDbCommand Command { get; }

        public Int32 CurrentResultIdx { get; set; } =  0;
        public Int32 CurrentRowIdx    { get; set; } = -1;

        public List<List<Object[]>> Results { get; set; } = new List<List<Object[]>>();

        public List<Object[]> Rows { get; set; } = new List<Object[]>();

        public Object[] RowData
        {
            get
            {
                return this.Rows[ this.CurrentRowIdx ];
            }
            set
            {
                this.Rows[ this.CurrentRowIdx ] = value ?? throw new ArgumentNullException(nameof(value));
            }
        }

        public String[] Names   { get; set; }

        public Type[]   Types   { get; set; }

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

        //

        public override bool NextResult()
        {
            // TODO: AllowSync/AllowAsync

            if( this.CurrentResultIdx < this.Results.Count - 1 )
            {
                this.CurrentResultIdx++;
                this.Rows          = this.Results[ this.CurrentResultIdx ];
                this.CurrentRowIdx = -1;
                return true;
            }
            else
            {
                return false;
            }
        }

        public override bool Read()
        {
            if( this.CurrentRowIdx < this.Rows.Count - 1 )
            {
                this.CurrentRowIdx++;
                return true;
            }
            else
            {
                return false;
            }
        }

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return base.NextResultAsync(cancellationToken);
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            return base.ReadAsync(cancellationToken);
        }
    }
}
