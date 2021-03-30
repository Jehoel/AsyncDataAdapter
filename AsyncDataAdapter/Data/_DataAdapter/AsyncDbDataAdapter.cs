using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    public abstract class AsyncDbDataAdapter<TDbCommand> : DbDataAdapter
        where TDbCommand : DbCommand
    {
        protected AsyncDbDataAdapter( DbDataAdapter adapter )
            : base( adapter )
        {
        }

        public new abstract TDbCommand SelectCommand { get; set; }

        protected abstract Task<Int32> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, TDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken );

        protected abstract Task<Int32> FillAsync( DataTable[] dataTables, int startRecord, int maxRecords, TDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken );

        #region Non-virtual entrypoints

        public Task<int> FillAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken = default)
        {
            return this.FillAsync( dataSet: dataSet, startRecord: 0, maxRecords: 0, srcTable: srcTable, command: this.SelectCommand, behavior: this.FillCommandBehavior, cancellationToken: cancellationToken );
        }

        public Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, CancellationToken cancellationToken = default)
        {
            return this.FillAsync( dataSet: dataSet, startRecord: startRecord, maxRecords: maxRecords, srcTable: srcTable, command: this.SelectCommand, behavior: this.FillCommandBehavior, cancellationToken: cancellationToken );
        }

        public Task<int> FillAsync(DataTable dataTable, CancellationToken cancellationToken = default)
        {
            DataTable[] dataTables = new DataTable[1] { dataTable };

            return this.FillAsync( dataTables, startRecord: 0, maxRecords: 0, command: this.SelectCommand, behavior: this.FillCommandBehavior, cancellationToken: cancellationToken );
        }

        public Task<int> FillAsync(int startRecord, int maxRecords, DataTable[] dataTables, CancellationToken cancellationToken = default)
        {
            return this.FillAsync( dataTables, startRecord: startRecord, maxRecords: maxRecords, command: this.SelectCommand, behavior: this.FillCommandBehavior, cancellationToken: cancellationToken );
        }

        #endregion
    }
}
