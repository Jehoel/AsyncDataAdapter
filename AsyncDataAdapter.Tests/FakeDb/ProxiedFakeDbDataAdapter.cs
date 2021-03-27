namespace AsyncDataAdapter.Tests
{
    public sealed class BatchingFakeProxiedDbDataAdapter : ProxyDbDataAdapter<BatchingFakeDbDataAdapter,FakeDbConnection,FakeDbCommand,FakeDbDataReader>
    {
        public BatchingFakeProxiedDbDataAdapter( FakeDbCommand selectCmd )
            : this( adp: new BatchingFakeDbDataAdapter( selectCmd ) )
        {

        }

        private BatchingFakeProxiedDbDataAdapter( BatchingFakeDbDataAdapter adp )
            : base( subject: adp, batchingAdapter: adp )
        {
        }
    }

    public sealed class NonBatchingFakeProxiedDbDataAdapter : ProxyDbDataAdapter<NonBatchingFakeDbDataAdapter,FakeDbConnection,FakeDbCommand,FakeDbDataReader>
    {
        public NonBatchingFakeProxiedDbDataAdapter( FakeDbCommand selectCmd )
            : base( subject: new NonBatchingFakeDbDataAdapter( selectCmd ), batchingAdapter: null )
        {
        }
    }
}
