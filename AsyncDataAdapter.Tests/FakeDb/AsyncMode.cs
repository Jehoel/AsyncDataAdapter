using System;

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
}
