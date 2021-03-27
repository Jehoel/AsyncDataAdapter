using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    /// <summary>NOTE: This <see cref="DbDataReader"/> does not implement <see cref="IBatchingAdapter"/>. For that, see <see cref="BatchingFakeDbDataAdapter"/>.</summary>
    public class FakeDbDataAdapter : DbDataAdapter
    {
        // TODO: Override every, single, method - and add call-counts.

        /// <summary>The <paramref name="select"/> is required before <see cref="DbDataAdapter.Fill(DataSet)"/> can be used.</summary>
        public FakeDbDataAdapter( FakeDbCommand select )
            : base()
        {
            this.SelectCommand = select ?? throw new ArgumentNullException(nameof(select));
        }

        /// <summary>The <paramref name="select"/> is required before <see cref="DbDataAdapter.Fill(DataSet)"/> can be used.</summary>
        public FakeDbDataAdapter( FakeDbCommand select, FakeDbCommand update, FakeDbCommand insert, FakeDbCommand delete )
            : this( select )
        {
            this.UpdateCommand = update ?? throw new ArgumentNullException(nameof(update));
            this.InsertCommand = insert ?? throw new ArgumentNullException(nameof(insert));
            this.DeleteCommand = delete ?? throw new ArgumentNullException(nameof(delete));
        }

        public FakeDbCommandBuilder CreateCommandBuilder()
        {
            return new FakeDbCommandBuilder( this );
        }
    }

    public class BatchingFakeDbDataAdapter : FakeDbDataAdapter /*DbDataAdapter*/, IBatchingAdapter
    {
        // TODO: Override every, single, method - and add call-counts.

        /// <summary>The <paramref name="select"/> is required before <see cref="DbDataAdapter.Fill(DataSet)"/> can be used.</summary>
        public BatchingFakeDbDataAdapter( FakeDbCommand select )
            : base( select )
        {
        }

        /// <summary>The <paramref name="select"/> is required before <see cref="DbDataAdapter.Fill(DataSet)"/> can be used.</summary>
        public BatchingFakeDbDataAdapter( FakeDbCommand select, FakeDbCommand update, FakeDbCommand insert, FakeDbCommand delete )
            : base( select, update, insert, delete )
        {
        }

        #region IBatchingAdapter

        MissingMappingAction IBatchingAdapter.UpdateMappingAction => this.UpdateMappingAction;
        MissingSchemaAction  IBatchingAdapter.UpdateSchemaAction  => this.UpdateSchemaAction;

        public MissingMappingAction UpdateMappingAction { get; set; }
        public MissingSchemaAction  UpdateSchemaAction  { get; set; }

        public List<DbCommand> BatchList { get; set; } = new List<DbCommand>();

        public int AddToBatch(DbCommand command)
        {
            this.BatchList.Add( command );
            return this.BatchList.Count;
        }

        void IBatchingAdapter.ClearBatch()
        {
            this.BatchList.Clear();
        }

        public async Task<int> ExecuteBatchAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        void IBatchingAdapter.TerminateBatching()
        {
            throw new NotImplementedException();
        }

        IDataParameter IBatchingAdapter.GetBatchedParameter(int commandIdentifier, int parameterIndex)
        {
            return this.BatchList[commandIdentifier].Parameters[parameterIndex];
        }

        bool IBatchingAdapter.GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error)
        {
            throw new NotImplementedException();
        }

        void IBatchingAdapter.InitializeBatching()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
