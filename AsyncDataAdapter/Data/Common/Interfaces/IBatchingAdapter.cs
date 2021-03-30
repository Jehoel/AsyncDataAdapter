using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    public interface IBatchingAdapter
    {
        MissingMappingAction UpdateMappingAction { get; }
        MissingSchemaAction  UpdateSchemaAction  { get; }

        int UpdateBatchSize { get; }

        /// <summary>Called to add a single command to the batch of commands that need to be executed as a batch, when batch updates are requested.  It must return an identifier that can be used to identify the command to GetBatchedParameter later.</summary>
        int AddToBatch( DbCommand command );

        /// <summary>Called when batch updates are requested to clear out the contents of the batch, whether or not it's been executed.</summary>
        void ClearBatch();

        /// <summary>Called to execute the batched update command, returns the number of rows affected, just as ExecuteNonQuery would.</summary>
        Task<int> ExecuteBatchAsync( CancellationToken cancellationToken );

        /// <summary>Called when batch updates are requested to cleanup after a batch update has been completed.</summary>
        void TerminateBatching();

        /// <summary>Called to retrieve a parameter from a specific bached command, the first argument is the value that was returned by AddToBatch when it was called for the command.</summary>
        IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex);

        bool GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error);

        /// <summary>Called when batch updates are requested to prepare for processing of a batch of commands.</summary>
        void InitializeBatching();
    }

    public interface IUpdatedRowOptions
    {
        Boolean AcceptChangesDuringUpdate { get; }
        Boolean ContinueUpdateOnError     { get; }
    }
}
