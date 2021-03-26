using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader>
    {
        private DataTableMapping GetTableMapping( DataTable dataTable )
        {
            IAdaSchemaMappingAdapter self = this;

            DataTableMapping tableMapping = null;
            int index = self.IndexOfDataSetTable(dataTable.TableName);
            if (-1 != index)
            {
                tableMapping = this.TableMappings[index];
            }

            if (null == tableMapping)
            {
                if (this.MissingMappingAction == MissingMappingAction.Error)
                {
                    throw ADP.MissingTableMappingDestination(dataTable.TableName);
                }

                tableMapping = new DataTableMapping(dataTable.TableName, dataTable.TableName);
            }

            return tableMapping;
        }

        #region UpdateAsync

        protected virtual RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            return new RowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
        }

        protected virtual RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            return new RowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
        }

 //     protected virtual void OnRowUpdated(RowUpdatedEventArgs value)
 //     {
 //     }
 //
 //     protected virtual void OnRowUpdating(RowUpdatingEventArgs value)
 //     {
 //     }

        public async Task<int> UpdateAsync(DataRow[] dataRows, CancellationToken cancellationToken)
        {
            int rowsAffected = 0;
            if (null == dataRows) throw new ArgumentNullException(nameof(dataRows));
            
            if (0 != dataRows.Length)
            {
                DataTable dataTable = null;
                for (int i = 0; i < dataRows.Length; ++i)
                {
                    if ((null != dataRows[i]) && (dataTable != dataRows[i].Table))
                    {
                        if (null != dataTable) throw new ArgumentException(string.Format("DataRow[{0}] is from a different DataTable than DataRow[0].", i));
                        dataTable = dataRows[i].Table;
                    }
                }

                if (null != dataTable)
                {
                    DataTableMapping tableMapping = this.GetTableMapping(dataTable);
                    rowsAffected = await this.UpdateAsync( dataRows, tableMapping, cancellationToken ).ConfigureAwait(false);
                }
            }

            return rowsAffected;
        }

        protected virtual Task<int> UpdateAsync( DataRow[] dataRows, DataTableMapping tableMapping, CancellationToken cancellationToken )
        {
            return AsyncDataReaderUpdateMethods.UpdateAsync( this, dataRows, tableMapping, cancellationToken );
        }

        public Task<int> UpdateAsync( DataTable dataTable, CancellationToken cancellationToken )
        {
            return AsyncDataReaderUpdateMethods.UpdateAsync( self: this, dataTable, cancellationToken );
        }

        public Task<int> UpdateAsync( DataSet dataSet, string srcTable, CancellationToken cancellationToken )
        {
            return AsyncDataReaderUpdateMethods.UpdateAsync( self: this, dataSet, srcTable, cancellationToken );
        }

        #endregion
    }
}
