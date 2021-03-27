using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    public class FakeDbCommand : DbCommand
    {
        public FakeDbCommand()
        {
        }

        public FakeDbCommand( FakeDbConnection c )
        {
            base.Connection = c;
        }

        public new FakeDbConnection Connection => (FakeDbConnection)base.Connection;

        public override void Cancel()
        {
        }

        protected override DbParameter CreateDbParameter()
        {
            return new FakeDbParameter();
        }

        #region Async Mode

        public AsyncMode AsyncMode { get; set; }

        public Boolean AllowSync  => this.AsyncMode.HasFlag( AsyncMode.AllowSync );
        
        public Boolean AllowAsync => this.AsyncMode.HasFlag( AsyncMode.AllowAsync );

        #endregion

        #region Execute

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return this.CreateReader( this );
        }

        public override Int32 ExecuteNonQuery()
        {
            if( this.AllowSync )
            {
                Thread.Sleep( 100 );

                return this.ExecuteNonQuery_Ret;
            }
            else
            {
                throw new NotSupportedException( "AllowSync is false." );
            }
        }

        public override Object ExecuteScalar()
        {
            if( this.AllowSync )
            {
                Thread.Sleep( 100 );

                return this.ExecuteScalar_Ret;
            }
            else
            {
                throw new NotSupportedException( "AllowSync is false." );
            }
        }

        #endregion

        #region ExecuteAsync

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            if( this.AsyncMode.HasFlag( AsyncMode.AwaitAsync ) )
            {
                await Task.Delay( 100 ).ConfigureAwait(false);

                return this.CreateReader( this );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.SyncAsync ) )
            {
                Thread.Sleep( 100 );

                return this.CreateReader( this );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.Default ) )
            {
                return await base.ExecuteDbDataReaderAsync( behavior, cancellationToken );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            if( this.AsyncMode.HasFlag( AsyncMode.AwaitAsync ) )
            {
                await Task.Delay( 100 ).ConfigureAwait(false);

                return this.ExecuteNonQuery_Ret;
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.SyncAsync ) )
            {
                Thread.Sleep( 100 );

                 return this.ExecuteNonQuery_Ret;
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.Default ) )
            {
                return await base.ExecuteNonQueryAsync( cancellationToken );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        public override async Task<Object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            if( this.AsyncMode.HasFlag( AsyncMode.AwaitAsync ) )
            {
                await Task.Delay( 100 ).ConfigureAwait(false);

                return this.ExecuteScalar_Ret;
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.SyncAsync ) )
            {
                Thread.Sleep( 100 );

                 return this.ExecuteScalar_Ret;
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.Default ) )
            {
                return await base.ExecuteScalarAsync( cancellationToken );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        #endregion

        public Func<FakeDbCommand,DbDataReader> CreateReader { get; set; } = cmd => new FakeDbDataReader( cmd );
        public Int32  ExecuteNonQuery_Ret;
        public Object ExecuteScalar_Ret;

        public override void Prepare()
        {
        }

        public    override String                CommandText           { get; set; }
        public    override Int32                 CommandTimeout        { get; set; }
        public    override CommandType           CommandType           { get; set; }
        protected override DbConnection          DbConnection          { get; set; }
        protected override DbTransaction         DbTransaction         { get; set; }
        public    override Boolean               DesignTimeVisible     { get; set; }
        public    override UpdateRowSource       UpdatedRowSource      { get; set; }
        
        protected override DbParameterCollection DbParameterCollection { get; } = new FakeDbParameterCollection();
    }
}
