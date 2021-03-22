using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    public abstract class AdaDataReaderContainer
    {
        public static AdaDataReaderContainer Create(DbDataReader dbDataReader)
        {
            if (dbDataReader is null) throw new ArgumentNullException(nameof(dbDataReader));

            return new ProviderSpecificDataReader( dbDataReader );
        }

        protected readonly DbDataReader _dataReader;
        protected int _fieldCount;

        protected AdaDataReaderContainer(DbDataReader dataReader)
        {
            this._dataReader = dataReader ?? throw new ArgumentNullException(nameof(dataReader));
        }

        internal int FieldCount => this._fieldCount;

        internal abstract bool ReturnProviderSpecificTypes { get; }
        protected abstract int VisibleFieldCount { get; }

        internal abstract Type GetFieldType(int ordinal);
        internal abstract object GetValue(int ordinal);
        internal abstract int GetValues(object[] values);

        internal string GetName(int ordinal)
        {
            string fieldName = _dataReader.GetName(ordinal);
            Debug.Assert(null != fieldName, "null GetName");
            return ((null != fieldName) ? fieldName : "");
        }
        internal DataTable GetSchemaTable()
        {
            return _dataReader.GetSchemaTable();
        }

        internal async Task<bool> NextResultAsync( CancellationToken cancellationToken )
        {
            this._fieldCount = 0;
            if (await this._dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false))
            {
                this._fieldCount = this.VisibleFieldCount;
                return true;
            }
            return false;
        }

        internal Task<bool> ReadAsync( CancellationToken cancellationToken )
        {
            return this._dataReader.ReadAsync( cancellationToken );
        }

        private sealed class ProviderSpecificDataReader : AdaDataReaderContainer
        {
            private DbDataReader _providerSpecificDataReader;

            internal ProviderSpecificDataReader( DbDataReader dbDataReader )
                : base( dbDataReader )
            {
                Debug.Assert(null != dbDataReader, "null dbDataReader");
                _providerSpecificDataReader = dbDataReader;
                _fieldCount = VisibleFieldCount;
            }

            internal override bool ReturnProviderSpecificTypes
            {
                get
                {
                    return true;
                }
            }
            protected override int VisibleFieldCount
            {
                get
                {
                    int fieldCount = _providerSpecificDataReader.VisibleFieldCount;
                    Debug.Assert(0 <= fieldCount, "negative FieldCount");
                    return ((0 <= fieldCount) ? fieldCount : 0);
                }
            }

            internal override Type GetFieldType(int ordinal)
            {
                Type fieldType = _providerSpecificDataReader.GetProviderSpecificFieldType(ordinal);
                Debug.Assert(null != fieldType, "null FieldType");
                return fieldType;
            }
            internal override object GetValue(int ordinal)
            {
                return _providerSpecificDataReader.GetProviderSpecificValue(ordinal);
            }
            internal override int GetValues(object[] values)
            {
                return _providerSpecificDataReader.GetProviderSpecificValues(values);
            }
        }

        /*
        private sealed class CommonLanguageSubsetDataReader : AdaDataReaderContainer
        {

            internal CommonLanguageSubsetDataReader(IDataReader dataReader)
                : base(dataReader)
            {
                _fieldCount = VisibleFieldCount;
            }

            internal override bool ReturnProviderSpecificTypes
            {
                get
                {
                    return false;
                }
            }
            protected override int VisibleFieldCount
            {
                get
                {
                    int fieldCount = _dataReader.FieldCount;
                    Debug.Assert(0 <= fieldCount, "negative FieldCount");
                    return ((0 <= fieldCount) ? fieldCount : 0);
                }
            }

            internal override Type GetFieldType(int ordinal)
            {
                return _dataReader.GetFieldType(ordinal);
            }
            internal override object GetValue(int ordinal)
            {
                return _dataReader.GetValue(ordinal);
            }
            internal override int GetValues(object[] values)
            {
                return _dataReader.GetValues(values);
            }
        }
        */
    }
}
