using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    public class NonBatchingFakeDbDataAdapter : DbDataAdapter
    {
        // TODO: Override every, single, method - and add call-counts.
    }

    public class BatchingFakeDbDataAdapter : DbDataAdapter, IBatchingAdapter
    {
        // TODO: Override every, single, method - and add call-counts.

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
