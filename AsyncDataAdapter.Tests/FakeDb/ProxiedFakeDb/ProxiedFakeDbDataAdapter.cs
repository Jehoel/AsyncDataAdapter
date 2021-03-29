using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter.Tests.FakeDb
{
    public sealed class FakeProxiedDbDataAdapter : ProxyDbDataAdapter<FakeDbDataAdapter,FakeDbConnection,FakeDbCommand,FakeDbDataReader>, IFullDbDataAdapter, IFullAsyncDbDataAdapter
    {
        public FakeProxiedDbDataAdapter( FakeDbCommand selectCmd )
            : base( subject: new FakeDbDataAdapter( selectCmd ), batchingAdapter: null )
        {
        }

        protected override DbCommandBuilder CreateCommandBuilder()
        {
            return new FakeDbCommandBuilder( this.Subject );
        }
    }

    public sealed class BatchingFakeProxiedDbDataAdapter : ProxyDbDataAdapter<BatchingFakeDbDataAdapter,FakeDbConnection,FakeDbCommand,FakeDbDataReader>, IFullDbDataAdapter, IFullAsyncDbDataAdapter
    {
        public BatchingFakeProxiedDbDataAdapter( FakeDbCommand selectCmd )
            : this( adp: new BatchingFakeDbDataAdapter( selectCmd ) )
        {

        }

        private BatchingFakeProxiedDbDataAdapter( BatchingFakeDbDataAdapter adp )
            : base( subject: adp, batchingAdapter: adp )
        {
        }

        protected override DbCommandBuilder CreateCommandBuilder()
        {
            return new FakeDbCommandBuilder( this.Subject );
        }
    }
}
