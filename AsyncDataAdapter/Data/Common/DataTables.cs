using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;

namespace AsyncDataAdapter.Internal
{
    /// <summary>Discriminated union (tagged union) type over <see cref="DataSet"/>, <see cref="DataTable"/>, and an array of <see cref="DataTable"/>.</summary>
    public struct DataTables : IReadOnlyCollection<DataTable>
    {
        private enum DataTablesType
        {
            Uninitialized,
            DataSet,
            DataTable,
            DataTables
        }

        public static implicit operator DataTables( DataSet dataSet )
        {
            return new DataTables( dataSet );
        }

        public static implicit operator DataTables( DataTable dataTable )
        {
            return new DataTables( dataTable );
        }

        public static implicit operator DataTables( DataTable[] dataTables )
        {
            return new DataTables( dataTables );
        }

        private readonly DataTablesType type;
        private readonly DataSet        dataSet;
        private readonly DataTable      dataTable;
        private readonly DataTable[]    dataTables;

        public DataTables( DataSet dataSet )
        {
            this.type       = DataTablesType.DataSet;
            this.dataSet    = dataSet ?? throw new ArgumentNullException(nameof(dataSet));
            this.dataTable  = null;
            this.dataTables = null;
        }

        public DataTables( DataTable dataTable )
        {
            this.type       = DataTablesType.DataTable;
            this.dataSet    = null;
            this.dataTable  = dataTable ?? throw new ArgumentNullException(nameof(dataTable));
            this.dataTables = null;
        }

        public DataTables( DataTable[] dataTables )
        {
            this.type       = DataTablesType.DataTables;
            this.dataSet    = null;
            this.dataTable  = null;
            this.dataTables = dataTables ?? throw new ArgumentNullException(nameof(dataTables));
        }

        public IEnumerable<DataTable> AsEnumerable()
        {
            switch (this.type)
            {
                case DataTablesType.DataSet:
                    return this.dataSet.Tables.Cast<DataTable>();

                case DataTablesType.DataTable:
                    return new DataTable[] { this.dataTable };

                case DataTablesType.DataTables:
                    return this.dataTables;

                case DataTablesType.Uninitialized:
                default:
                    throw new InvalidOperationException( "This " + nameof(DataTables) + " tagged union is not initialized correctly." );
            }
        }

        #region IReadOnlyCollection<DataTable>

        public Int32 Count
        {
            get
            {
                switch (this.type)
                {
                    case DataTablesType.DataSet:
                        return this.dataSet.Tables.Count;

                    case DataTablesType.DataTable:
                        return 1;

                    case DataTablesType.DataTables:
                        return this.dataTables.Length;

                    case DataTablesType.Uninitialized:
                    default:
                        throw new InvalidOperationException( "This " + nameof(DataTables) + " tagged union is not initialized correctly." );
                }
            }
        }

        public IEnumerator<DataTable> GetEnumerator()
        {
            return this.AsEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.AsEnumerable().GetEnumerator();
        }

        #endregion
    }
}
