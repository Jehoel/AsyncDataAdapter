using System;
using System.ComponentModel;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    public static class AsyncDataReaderConnectionMethods
    {
        /// <summary></summary>
        /// <remarks><see cref="QuietOpenAsync"/> needs to appear in the try {} finally { QuietClose } block otherwise a possibility exists that an exception may be thrown, i.e. <see cref="ThreadAbortException"/> where we would Open the connection and not close it</remarks>
        /// <returns></returns>
        public static async Task<ConnectionState> QuietOpenAsync( DbConnection connection, CancellationToken cancellationToken )
        {
            Debug.Assert(null != connection, "QuietOpen: null connection");
            var originalState = connection.State;
            if (ConnectionState.Closed == originalState)
            {
                await connection.OpenAsync( cancellationToken ).ConfigureAwait(false);
            }

            return originalState;
        }

        public static void QuietClose( DbConnection connection, ConnectionState originalState )
        {
            // close the connection if:
            // * it was closed on first use and adapter has opened it, AND
            // * provider's implementation did not ask to keep this connection open
            if ((null != connection) && (ConnectionState.Closed == originalState))
            {
                // we don't have to check the current connection state because
                // it is supposed to be safe to call Close multiple times
                connection.Close();
            }
        }
    }
}
