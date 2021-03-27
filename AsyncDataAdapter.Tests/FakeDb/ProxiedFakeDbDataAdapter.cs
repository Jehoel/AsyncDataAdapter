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

        public FakeDbCommandBuilder CreateCommandBuilder()
        {
            return new FakeDbCommandBuilder( this.Subject );
        }
    }

    public sealed class NonBatchingFakeProxiedDbDataAdapter : ProxyDbDataAdapter<FakeDbDataAdapter,FakeDbConnection,FakeDbCommand,FakeDbDataReader>
    {
        public NonBatchingFakeProxiedDbDataAdapter( FakeDbCommand selectCmd )
            : base( subject: new FakeDbDataAdapter( selectCmd ), batchingAdapter: null )
        {
        }

        public FakeDbCommandBuilder CreateCommandBuilder()
        {
            return new FakeDbCommandBuilder( this.Subject );
        }
    }
}
