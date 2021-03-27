using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    // hmmm, need to subclass `DbProviderFactory` too?

    public class FakeDbConnection : DbConnection
    {
        public FakeDbConnection( AsyncMode asyncMode = AsyncMode.AwaitAsync, Int32 inducedDelayMS = 100 )
        {
            this.AsyncMode    = asyncMode;
            this.InducedDelay = TimeSpan.FromMilliseconds( inducedDelayMS );
        }

        public override String ConnectionString { get; set; }

        public override String Database => this.Database2;

        public String Database2 { get; set; } = "FakeDatabase";

        public override String DataSource => "FakeDataSource";

        public override String ServerVersion => "1.2.3";

        public ConnectionState State2 { get; set; } = ConnectionState.Closed;

        public override ConnectionState State => this.State2;

        #region Test data

        public AsyncMode AsyncMode { get; set; }

        public TimeSpan InducedDelay { get; set; }

        #endregion

        #region CreateCommand

        protected override DbCommand CreateDbCommand()
        {
            return this.CreateCommand();
        }

        public new FakeDbCommand CreateCommand()
        {
            return this.CreateCommand( testTables: null );
        }

        public FakeDbCommand CreateCommand( List<TestTable> testTables )
        {
            return new FakeDbCommand( connection: this, testTables: testTables ) { AsyncMode = this.AsyncMode };
        }

        #endregion

        //

        public override void Open()
        {
            if( this.AsyncMode.AllowOld() )
            {
                Thread.Sleep( 100 );

                this.State2 = ConnectionState.Open; 
            }
            else
            {
                throw new NotSupportedException( "AllowSync is false." );
            }
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if( this.AsyncMode.HasFlag( AsyncMode.AwaitAsync ) )
            {
                await Task.Delay( 100 ).ConfigureAwait(false);

                this.State2 = ConnectionState.Open; 
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                this.State2 = ConnectionState.Open; 
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                await base.OpenAsync();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                await Task.Run( () => { this.State2 = ConnectionState.Open; } );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        public override void Close()
        {
            this.State2 = ConnectionState.Closed;
        }

        #region BeginDbTransaction

        private DbTransaction BeginDbTransactionImpl( IsolationLevel isolationLevel )
        {
            return new FakeDbTransaction( this, isolationLevel );
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if( this.AsyncMode.AllowOld() )
            {
                Thread.Sleep( 100 );

                return this.BeginDbTransactionImpl( isolationLevel );
            }
            else
            {
                throw new NotSupportedException( "AllowSync is false." );
            }
        }

        protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        {
            if( this.AsyncMode.HasFlag( AsyncMode.AwaitAsync ) )
            {
                await Task.Delay( 100 ).ConfigureAwait(false);

                return this.BeginDbTransactionImpl( isolationLevel );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                return this.BeginDbTransactionImpl( isolationLevel );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                return await base.BeginDbTransactionAsync( isolationLevel, cancellationToken );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                return await Task.Run( () => this.BeginDbTransactionImpl( isolationLevel ) );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        #endregion

        public override void ChangeDatabase(String databaseName)
        {
            this.Database2 = databaseName;
        }

        // Don't override CloseAsync, it isn't in .NET Standard 2.0.
    }
}
