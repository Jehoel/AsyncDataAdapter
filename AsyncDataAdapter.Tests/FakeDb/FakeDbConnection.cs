using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    public class FakeDbConnection : DbConnection
    {
        public FakeDbConnection()
        {
        }

        public override String ConnectionString { get; set; }

        public override String Database => this.Database2;

        public String Database2 { get; set; } = "FakeDatabase";

        public override String DataSource => "FakeDataSource";

        public override String ServerVersion => "1.2.3";

        public ConnectionState State2 { get; set; } = ConnectionState.Closed;

        public override ConnectionState State => this.State2;

        //

        public AsyncMode AsyncMode { get; set; }

        public Boolean AllowSync  => this.AsyncMode.HasFlag( AsyncMode.AllowSync );
        
        public Boolean AllowAsync => this.AsyncMode.HasFlag( AsyncMode.AllowAsync );

        //

        public override void Open()
        {
            if( this.AllowSync )
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
            else if( this.AsyncMode.HasFlag( AsyncMode.SyncAsync ) )
            {
                Thread.Sleep( 100 );

                this.State2 = ConnectionState.Open; 
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.Default ) )
            {
                await base.OpenAsync();
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

        protected override DbCommand CreateDbCommand()
        {
            return new FakeDbCommand();
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if( this.AllowSync )
            {
                Thread.Sleep( 100 );

                return new FakeDbTransaction( this, isolationLevel );
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

                return new FakeDbTransaction( this, isolationLevel );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.SyncAsync ) )
            {
                Thread.Sleep( 100 );

                return new FakeDbTransaction( this, isolationLevel );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.Default ) )
            {
                return await base.BeginDbTransactionAsync( isolationLevel, cancellationToken );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        public override void ChangeDatabase(String databaseName)
        {
            this.Database2 = databaseName;
        }

        public override Task CloseAsync()
        {
            return base.CloseAsync();
        }
    }
}
