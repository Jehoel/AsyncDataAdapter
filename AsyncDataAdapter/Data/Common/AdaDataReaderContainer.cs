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
        public static AdaDataReaderContainer Create(DbDataReader dbDataReader, bool useProviderSpecificDataReader)
        {
            if (dbDataReader is null) throw new ArgumentNullException(nameof(dbDataReader));

            if (useProviderSpecificDataReader)
            {
                return new ProviderSpecificDataReader( dbDataReader );
            }
            else
            {
                return new CommonLanguageSubsetDataReader( dbDataReader );
            }
        }

        protected readonly DbDataReader dataReader;
        protected int fieldCount;

        protected AdaDataReaderContainer(DbDataReader dataReader)
        {
            this.dataReader = dataReader ?? throw new ArgumentNullException(nameof(dataReader));
        }

        internal int FieldCount => this.fieldCount;

        internal abstract bool ReturnProviderSpecificTypes { get; }
        protected abstract int VisibleFieldCount { get; }

        internal abstract Type GetFieldType(int ordinal);
        internal abstract object GetValue(int ordinal);
        internal abstract int GetValues(object[] values);

        internal string GetName(int ordinal)
        {
            string fieldName = this.dataReader.GetName(ordinal);
            Debug.Assert(null != fieldName, "null GetName");
            return fieldName ?? "";
        }
        internal DataTable GetSchemaTable()
        {
            return this.dataReader.GetSchemaTable();
        }

        internal async Task<bool> NextResultAsync( CancellationToken cancellationToken )
        {
            this.fieldCount = 0;
            if (await this.dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false))
            {
                this.fieldCount = this.VisibleFieldCount;
                return true;
            }
            return false;
        }

        internal Task<bool> ReadAsync( CancellationToken cancellationToken )
        {
            return this.dataReader.ReadAsync( cancellationToken );
        }
    }

    /// <summary>Will return values using the ADO.NET provider type. For example: with <c>System.Data.SqlClient</c> a <see cref="DbType.Double"/> value (<c>float</c> in T-SQL) will be returned as <see cref="System.Data.SqlTypes.SqlDouble"/> instead of as a <see cref="Double"/> value.</summary>
    public sealed class ProviderSpecificDataReader : AdaDataReaderContainer
    {
        private readonly DbDataReader providerSpecificDataReader;

        internal ProviderSpecificDataReader( DbDataReader dbDataReader )
            : base( dbDataReader )
        {
            Debug.Assert(null != dbDataReader, "null dbDataReader");
            this.providerSpecificDataReader = dbDataReader;
            this.fieldCount = this.VisibleFieldCount;
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
                int fieldCount = this.providerSpecificDataReader.VisibleFieldCount;
                Debug.Assert(0 <= fieldCount, "negative FieldCount");
                return ((0 <= fieldCount) ? fieldCount : 0);
            }
        }

        internal override Type GetFieldType(int ordinal)
        {
            Type fieldType = this.providerSpecificDataReader.GetProviderSpecificFieldType(ordinal);
            Debug.Assert(null != fieldType, "null FieldType");
            return fieldType;
        }
        internal override object GetValue(int ordinal)
        {
            return this.providerSpecificDataReader.GetProviderSpecificValue(ordinal);
        }
        internal override int GetValues(object[] values)
        {
            return this.providerSpecificDataReader.GetProviderSpecificValues(values);
        }
    }

    /// <summary>Will return values as, for example, <see cref="System.Double"/> instead of <see cref="System.Data.SqlTypes.SqlDouble"/>.</summary>
    /// <summary>Will return values using CLS types instead of any ADO.NET provider-specific types. For example: with <c>System.Data.SqlClient</c> a <see cref="DbType.Double"/> value (<c>float</c> in T-SQL) will be returned as <see cref="Double"/>  value and NOT as a <see cref="System.Data.SqlTypes.SqlDouble"/>  value.</summary>
    public sealed class CommonLanguageSubsetDataReader : AdaDataReaderContainer
    {
        internal CommonLanguageSubsetDataReader(DbDataReader dbDataReader)
            : base(dbDataReader)
        {
            this.fieldCount = this.VisibleFieldCount;
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
                int fieldCount = this.dataReader.FieldCount;
                Debug.Assert(0 <= fieldCount, "negative FieldCount");
                return ((0 <= fieldCount) ? fieldCount : 0);
            }
        }

        internal override Type GetFieldType(int ordinal)
        {
            return this.dataReader.GetFieldType(ordinal);
        }
        internal override object GetValue(int ordinal)
        {
            return this.dataReader.GetValue(ordinal);
        }
        internal override int GetValues(object[] values)
        {
            return this.dataReader.GetValues(values);
        }
    }
}
