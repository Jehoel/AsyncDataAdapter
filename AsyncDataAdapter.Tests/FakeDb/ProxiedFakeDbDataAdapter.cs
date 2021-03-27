namespace AsyncDataAdapter.Tests
{
    public sealed class BatchingFakeProxiedDbDataAdapter : ProxyDbDataAdapter<BatchingFakeDbDataAdapter,FakeDbConnection,FakeDbCommand,FakeDbDataReader>
    {
        public BatchingFakeProxiedDbDataAdapter()
            : this( adp: new BatchingFakeDbDataAdapter() )
        {

        }

        private BatchingFakeProxiedDbDataAdapter( BatchingFakeDbDataAdapter adp )
            : base( subject: adp, batchingAdapter: adp )
        {
        }
    }

    public sealed class NonBatchingFakeProxiedDbDataAdapter : ProxyDbDataAdapter<NonBatchingFakeDbDataAdapter,FakeDbConnection,FakeDbCommand,FakeDbDataReader>
    {
        public NonBatchingFakeProxiedDbDataAdapter()
            : base( subject: new NonBatchingFakeDbDataAdapter(), batchingAdapter: null )
        {
        }
    }
}
