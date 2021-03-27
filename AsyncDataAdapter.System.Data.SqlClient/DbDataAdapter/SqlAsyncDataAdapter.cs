using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    using ProxyDbDataAdapterForSqlClient = ProxyDbDataAdapter<
        SqlDataAdapter,
        SqlConnection,
        SqlCommand,
        SqlDataReader
    >;

    /// <summary>For use with <see cref="System.Data.SqlClient.SqlDataAdapter"/>.</summary>
    public sealed class SqlAsyncDbDataAdapter : ProxyDbDataAdapterForSqlClient
    {
        public SqlAsyncDbDataAdapter()
            : this( original: new SqlDataAdapter() )
        {
        }

        public SqlAsyncDbDataAdapter( SqlCommand selectCommand )
            : this( original: new SqlDataAdapter( selectCommand ) )
        {
        }

        public SqlAsyncDbDataAdapter( String selectCommandText, SqlConnection connection )
            : this( original: new SqlDataAdapter( selectCommandText, connection ) )
        {
        }

        public SqlAsyncDbDataAdapter( String selectCommandText, String connectionString )
            : this( original: new SqlDataAdapter( selectCommandText, connectionString ) )
        {
        }

        //

        private SqlAsyncDbDataAdapter( SqlDataAdapter original )
            : this( original: original, batching: new BatchingSqlDataAdapter( original ) )
        {

        }

        private SqlAsyncDbDataAdapter( SqlDataAdapter original, BatchingSqlDataAdapter batching )
            : base( batchingAdapter: batching, subject: original )
        {
        }
    }
}

namespace AsyncDataAdapter.Internal
{
    using global::AsyncDataAdapter.Internal;
    using Ref = global::AsyncDataAdapter.Internal.Reflection;

    using System.Reflection;

    public class BatchingSqlDataAdapter : IBatchingAdapter
    {
        private static readonly MethodInfo _UpdateMappingAction_Get   = Ref.GetInstancePropertyGetter<SqlDataAdapter>( name: nameof(UpdateMappingAction), propertyType: typeof(MissingMappingAction) );
        private static readonly MethodInfo _UpdateSchemaAction_Get    = Ref.GetInstancePropertyGetter<SqlDataAdapter>( name: nameof(UpdateSchemaAction) , propertyType: typeof(MissingSchemaAction)  );
        private static readonly MethodInfo _UpdateBatchSize_Get       = Ref.GetInstancePropertyGetter<SqlDataAdapter>( name: nameof(UpdateBatchSize)    , propertyType: typeof(Int32)                );

        private static readonly MethodInfo _AddToBatch                = Ref.GetInstanceMethod<SqlDataAdapter>( name: nameof(AddToBatch)               , typeof(DbCommand) );
        private static readonly MethodInfo _ClearBatch                = Ref.GetInstanceMethod<SqlDataAdapter>( name: nameof(ClearBatch)               );
        private static readonly MethodInfo _ExecuteBatchAsync         = Ref.GetInstanceMethod<SqlDataAdapter>( name: nameof(ExecuteBatchAsync)        , typeof(CancellationToken) );
        private static readonly MethodInfo _TerminateBatching         = Ref.GetInstanceMethod<SqlDataAdapter>( name: nameof(TerminateBatching)        );
        private static readonly MethodInfo _GetBatchedParameter       = Ref.GetInstanceMethod<SqlDataAdapter>( name: nameof(GetBatchedParameter)      , typeof(Int32), typeof(Int32) );
        private static readonly MethodInfo _GetBatchedRecordsAffected = Ref.GetInstanceMethod<SqlDataAdapter>( name: nameof(GetBatchedRecordsAffected), typeof(Int32), typeof(Int32), typeof(Exception) );
        private static readonly MethodInfo _InitializeBatching        = Ref.GetInstanceMethod<SqlDataAdapter>( name: nameof(InitializeBatching)       );

        //

        private readonly SqlDataAdapter adapter;

        public BatchingSqlDataAdapter( SqlDataAdapter adapter )
        {
            this.adapter = adapter ?? throw new ArgumentNullException(nameof(adapter));
        }

        public MissingMappingAction UpdateMappingAction => _UpdateMappingAction_Get.InvokeDisallowNull<MissingMappingAction>( this.adapter );
        public MissingSchemaAction  UpdateSchemaAction  => _UpdateSchemaAction_Get .InvokeDisallowNull<MissingSchemaAction> ( this.adapter );
        public Int32                UpdateBatchSize     => _UpdateBatchSize_Get    .InvokeDisallowNull<Int32>               ( this.adapter );

        public int AddToBatch(DbCommand command)
        {
            return _AddToBatch.InvokeDisallowNull<Int32>( this.adapter, command );
        }

        public void ClearBatch()
        {
            _ClearBatch.InvokeVoid( this.adapter );
        }

        public Task<int> ExecuteBatchAsync(CancellationToken cancellationToken)
        {
            return _ExecuteBatchAsync.InvokeDisallowNull<Task<int>>( this.adapter, cancellationToken );
        }

        public void TerminateBatching()
        {
            _TerminateBatching.InvokeVoid( this.adapter );
        }

        public IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex)
        {
            return _GetBatchedParameter.InvokeDisallowNull<IDataParameter>( this.adapter, commandIdentifier, parameterIndex );
        }

        public bool GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error)
        {
            Object[] paramsArgs = new Object[]
            {
                commandIdentifier,
                null,
                null
            };

            Boolean ok = _GetBatchedRecordsAffected.InvokeDisallowNull<bool>( this.adapter, paramsArgs );

            recordsAffected =     (Int32)paramsArgs[1];
            error           = (Exception)paramsArgs[2];

            return ok;
        }

        public void InitializeBatching()
        {
            _InitializeBatching.InvokeVoid( this.adapter );
        }
    }
}
