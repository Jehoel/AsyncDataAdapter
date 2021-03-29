using System;
using System.Collections.Generic;
using System.Text;

namespace AsyncDataAdapter.Tests.FakeDb
{
    public class FakeDbDelays
    {
        public static FakeDbDelays DefaultDelaysNone { get; } = new FakeDbDelays(
            connect : null,
            execute : null,
            transact: null,
            result  : null,
            row     : null
        );

        public static FakeDbDelays DefaultDelays1MS { get; } = new FakeDbDelays(
            connect : TimeSpan.FromMilliseconds(1),
            execute : TimeSpan.FromMilliseconds(1),
            transact: TimeSpan.FromMilliseconds(1),
            result  : TimeSpan.FromMilliseconds(1),
            row     : TimeSpan.FromMilliseconds(1)
        );

        public FakeDbDelays(TimeSpan? connect, TimeSpan? execute, TimeSpan? transact, TimeSpan? result, TimeSpan? row)
        {
            this.Connect  = connect;
            this.Execute  = execute;
            this.Transact = transact;
            this.Result   = result;
            this.Row      = row;
        }

        public TimeSpan? Connect  { get; }
        public TimeSpan? Execute  { get; }
        public TimeSpan? Transact { get; }
        public TimeSpan? Result   { get; }
        public TimeSpan? Row      { get; }
    }
}
