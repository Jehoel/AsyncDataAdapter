using System;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    [Flags]
    public enum AsyncMode
    {
        None        = 0,

        AllowSync   = 1,

        /// <summary>Do real async work, or if that's not possible then emulate it by awaiting <see cref="Task.Delay(int)"/> and then calling the synchronous implementation, for example.</summary>
        AwaitAsync  = 2 |  4,

        /// <summary>Block the thread using <see cref="Thread.Sleep(int)"/> and then directly call the synchronous version. This is meant to simulate crappy fake-async implementations.</summary>
        BlockAsync   = 2 | 8,

        /// <summary>If the async method is virtual and already implemented by the framework superclass (e.g. <see cref="System.Data.Common.DbDataReader.ReadAsync"/>) then call that method.</summary>
        BaseAsync   = 2 | 16,

        /// <summary>Run the logic (sync or async) inside a <see cref="Task.Run(Action)"/> job.</summary>
        RunAsync    = 2 | 32,

        // This enum design is probably wrong - the 4/8/16 options are meant to be mutually-exclusive, ugh.
    }

    public static class Extensions
    {
        /// <summary>Not named <c>AllowSync</c> because it's too similar to <see cref="AllowAsync(AsyncMode)"/>.</summary>
        public static Boolean AllowOld( this AsyncMode value ) => ( (Int32)value & 1 ) == 1;

        public static Boolean AllowAsync( this AsyncMode value ) => ( (Int32)value & 2 ) == 2;

        /*
        public static async Task<TResult> RunAsync<TResult>( this AsyncMode mode, Func<TResult> syncMethod )
        {
            if( mode.HasFlag( AsyncMode.AwaitAsync ) )
            {
                await Task.Delay( 100 ).ConfigureAwait(false);

                return syncMethod()
            }
            else if( mode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                return this.CreateReader( this );
            }
            else if( mode.HasFlag( AsyncMode.BaseAsync ) )
            {
                Thread.Sleep( 100 );

                return await base.ExecuteDbDataReaderAsync( behavior, cancellationToken );
            }
            else if( tmode.HasFlag( AsyncMode.RunAsync ) )
            {
                await Task.Yield();

                return await Task.Run( () => this.CreateReader( this ) );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        /*
        public static Boolean AllowAsync( this AsyncMode value, out Boolean useAwait, out Boolean useBlock, out Boolean useBase, out Boolean useTaskRun )
        {
            if( ( (Int32)value & 2 ) == 2 )
            {
                useAwait   = value.HasFlag( AsyncMode.AwaitAsync );
                useBlock   = value.HasFlag( AsyncMode.BlockAsync );
                useBase    = value.HasFlag( AsyncMode.BaseAsync  );
                useTaskRun = value.HasFlag( AsyncMode.RunAsync   );

                return true;
            }
            else
            {
                useAwait   = false;
                useBlock   = false;
                useBase    = false;
                useTaskRun = false;
            }

            return false;
        }
        */
    }
}
