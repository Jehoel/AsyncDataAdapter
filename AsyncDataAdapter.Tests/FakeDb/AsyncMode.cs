using System;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    [Flags]
    public enum AsyncMode
    {
        None       = 0,
        AllowSync  = 1,
        AllowAsync = 2,

        AwaitAsync = AllowAsync |  4,
        SyncAsync  = AllowAsync |  8,
        Default    = AllowAsync | 16,
    }

//    public class AsyncMixin
//    {
//        public AsyncMode AsyncMode { get; }
//
//        public Boolean AllowSync  => this.AsyncMode.HasFlag( AsyncMode.AllowSync );
//        
//        public Boolean AllowAsync => this.AsyncMode.HasFlag( AsyncMode.AllowAsync );
//
//        public async Task DoThingAsync( CancellationToken cancellationToken, Func<Task> )
//        {
//
//        }
//    }
}
