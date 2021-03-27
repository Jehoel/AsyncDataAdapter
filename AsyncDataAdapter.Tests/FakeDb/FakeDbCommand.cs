using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests
{
    public class FakeDbCommand : DbCommand
    {
        /// <summary>NOTE: When using this constructor, ensure the <see cref="DbCommand.Connection"/> property is set before <see cref="DbDataAdapter.Fill(DataSet)"/> (or other overloads) are called.</summary>
        [Obsolete( "(Not actually obsolete, this attribute is just to warn you to not use this ctor unless you really know you need to)" )]
        public FakeDbCommand()
        {
        }

        public FakeDbCommand( FakeDbConnection connection, List<TestTable> testTables )
        {
            base.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.TestTables = testTables;
        }

        #region Overridden

        public    override String                CommandText           { get; set; } // Base is abstract.
        public    override Int32                 CommandTimeout        { get; set; } // Base is abstract.
        public    override CommandType           CommandType           { get; set; } // Base is abstract.
        protected override DbConnection          DbConnection          { get; set; } // Base is abstract. The public one is non-virtual and directly reads/writes the protected abstract property (i.e. this one).
        protected override DbTransaction         DbTransaction         { get; set; } // Base is abstract.
        public    override Boolean               DesignTimeVisible     { get; set; } // Base is abstract.
        public    override UpdateRowSource       UpdatedRowSource      { get; set; } // Base is abstract.
        
        protected override DbParameterCollection DbParameterCollection { get; } = new FakeDbParameterCollection();

        //

        public new FakeDbConnection Connection => (FakeDbConnection)base.Connection;

        #endregion

        #region Test Data

        /// <summary>Used to prepopulate any <see cref="FakeDbDataReader"/> that's created.</summary>
        public List<TestTable> TestTables { get; set; }

        public AsyncMode AsyncMode { get; set; }

        private FakeDbDataReader CreateFakeDbDataReader()
        {
            FakeDbDataReader reader = new FakeDbDataReader( cmd: this );
            if( this.TestTables != null )
            {
                reader.ResetAndLoadTestData( this.TestTables );
            }

            return reader;
        }

        public Func<FakeDbCommand,DbDataReader> CreateReader { get; set; }

        public Int32  ExecuteNonQuery_Ret;
        public Object ExecuteScalar_Ret;

        #endregion

        #region Misc

        public override void Cancel()
        {
        }

        protected override DbParameter CreateDbParameter()
        {
            return new FakeDbParameter();
        }

        public override void Prepare()
        {
        }

        #endregion

        #region Execute

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if( this.AsyncMode.AllowOld() )
            {
                Thread.Sleep( 100 );

                return this.CreateFakeDbDataReader();
            }
            else
            {
                throw new NotSupportedException( "AllowSync is false." );
            }
        }

        public override Int32 ExecuteNonQuery()
        {
            if( this.AsyncMode.AllowOld() )
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
             if( this.AsyncMode.AllowOld() )
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
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                return this.CreateReader( this );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                Thread.Sleep( 100 );

                return await base.ExecuteDbDataReaderAsync( behavior, cancellationToken );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                await Task.Yield();

                return await Task.Run( () => this.CreateReader( this ) );
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
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                return this.ExecuteNonQuery_Ret;
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                Thread.Sleep( 100 );

                return await base.ExecuteNonQueryAsync( cancellationToken );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                await Task.Yield();

                return await Task.Run( () => this.ExecuteNonQuery_Ret );
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
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                return this.ExecuteScalar_Ret;
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                Thread.Sleep( 100 );

                return await base.ExecuteScalarAsync( cancellationToken );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                await Task.Yield();

                return await Task.Run( () => this.ExecuteScalar_Ret );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        #endregion
    }
}
