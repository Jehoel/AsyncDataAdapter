using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.Internal
{
    public class BatchingMSqlDataAdapter : IBatchingAdapter
    {
        private readonly SqlDataAdapter adapter;

        public BatchingMSqlDataAdapter( SqlDataAdapter adapter )
        {
            this.adapter = adapter ?? throw new ArgumentNullException(nameof(adapter));
        }

        private struct _UpdateMappingAction { }
        private struct _UpdateSchemaAction  { }
        private struct _UpdateBatchSize     { }

        public MissingMappingAction UpdateMappingAction => ReflectedProperty<SqlDataAdapter,_UpdateMappingAction,MissingMappingAction>.GetValue( this.adapter );
        public MissingSchemaAction  UpdateSchemaAction  => ReflectedProperty<SqlDataAdapter,_UpdateSchemaAction ,MissingSchemaAction >.GetValue( this.adapter );
        public Int32                UpdateBatchSize     => ReflectedProperty<SqlDataAdapter,_UpdateBatchSize    ,Int32               >.GetValue( this.adapter );

        private struct _AddToBatch { }

        public int AddToBatch(DbCommand command)
        {
            return ReflectedFunc<SqlDataAdapter,_AddToBatch,DbCommand,Int32>.Invoke( this.adapter, command );
        }

        private struct _ClearBatch { }

        public void ClearBatch()
        {
            ReflectedAction<SqlDataAdapter,_ClearBatch>.Invoke( this.adapter );
        }

        private struct _ExecuteBatchAsync { }

        public Task<int> ExecuteBatchAsync(CancellationToken cancellationToken)
        {
            return ReflectedFunc<SqlDataAdapter,_ExecuteBatchAsync,CancellationToken,Task<Int32>>.Invoke( this.adapter, cancellationToken );
        }

        private struct _TerminateBatching { }

        public void TerminateBatching()
        {
            ReflectedAction<SqlDataAdapter,_TerminateBatching>.Invoke( this.adapter );
        }

        private struct _GetBatchedParameter { }

        public IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex)
        {
            return ReflectedFunc<SqlDataAdapter,_GetBatchedParameter,Int32,Int32,IDataParameter>.Invoke( this.adapter, commandIdentifier, parameterIndex );
        }

        private struct _GetBatchedRecordsAffected { }

        public bool GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error)
        {
            return ReflectedFuncO2O3<SqlDataAdapter,_GetBatchedRecordsAffected,Int32,Int32,Exception,Boolean>.Invoke( this.adapter, commandIdentifier, out recordsAffected, out error );
        }

        private struct _InitializeBatching { }

        public void InitializeBatching()
        {
            ReflectedAction<SqlDataAdapter,_InitializeBatching>.Invoke( this.adapter );
        }
    }
}
