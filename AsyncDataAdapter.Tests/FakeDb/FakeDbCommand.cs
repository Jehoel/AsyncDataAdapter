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
            this.CreateReader = this.CreateFakeDbDataReader;
        }

        public FakeDbCommand( FakeDbConnection connection, List<TestTable> testTables )
        {
            base.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.TestTables = testTables;

            this.CreateReader = this.CreateFakeDbDataReader;
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

        private FakeDbDataReader CreateFakeDbDataReader( FakeDbCommand cmd )
        {
            FakeDbDataReader reader = new FakeDbDataReader( cmd: cmd );
            if( this.TestTables != null )
            {
                reader.ResetAndLoadTestData( this.TestTables );
            }

            return reader;
        }

        private FakeDbDataReader CreateFakeDbDataReader()
        {
            return this.CreateFakeDbDataReader( cmd: this );
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

        private Int32 GetNonQueryResultRowCount()
        {
            // Special-case for UpdateCommands from DbDataAdapter and DbCommandBuilder:
            // If the query looks like this:
            /*

            UPDATE [Table_1] SET [PK] = @p1, [Col1] = @p2, [Col2] = @p3, [Col3] = @p4, [Col4] = @p5, [Col5] = @p6, [Col6] = @p7, [Col7] = @p8, [Col8] = @p9, [Col9] = @p10, [Col10] = @p11, [Col11] = @p12, [Col12] = @p13, [Col13] = @p14, [Col14] = @p15, [Col15] = @p16, [Col16] = @p17, [Col17] = @p18, [Col18] = @p19, [Col19] = @p20, [Col20] = @p21, [Col21] = @p22, [Col22] = @p23 WHERE (([PK] = @p24) AND ((@p25 = 1 AND [Col1] IS NULL) OR ([Col1] = @p26)) AND ((@p27 = 1 AND [Col2] IS NULL) OR ([Col2] = @p28)) AND ((@p29 = 1 AND [Col3] IS NULL) OR ([Col3] = @p30)) AND ((@p31 = 1 AND [Col4] IS NULL) OR ([Col4] = @p32)) AND ((@p33 = 1 AND [Col5] IS NULL) OR ([Col5] = @p34)) AND ((@p35 = 1 AND [Col6] IS NULL) OR ([Col6] = @p36)) AND ((@p37 = 1 AND [Col7] IS NULL) OR ([Col7] = @p38)) AND ((@p39 = 1 AND [Col8] IS NULL) OR ([Col8] = @p40)) AND ((@p41 = 1 AND [Col9] IS NULL) OR ([Col9] = @p42)) AND ((@p43 = 1 AND [Col10] IS NULL) OR ([Col10] = @p44)) AND ((@p45 = 1 AND [Col11] IS NULL) OR ([Col11] = @p46)) AND ((@p47 = 1 AND [Col12] IS NULL) OR ([Col12] = @p48)) AND ((@p49 = 1 AND [Col13] IS NULL) OR ([Col13] = @p50)) AND ((@p51 = 1 AND [Col14] IS NULL) OR ([Col14] = @p52)) AND ((@p53 = 1 AND [Col15] IS NULL) OR ([Col15] = @p54)) AND ((@p55 = 1 AND [Col16] IS NULL) OR ([Col16] = @p56)) AND ((@p57 = 1 AND [Col17] IS NULL) OR ([Col17] = @p58)) AND ((@p59 = 1 AND [Col18] IS NULL) OR ([Col18] = @p60)) AND ((@p61 = 1 AND [Col19] IS NULL) OR ([Col19] = @p62)) AND ((@p63 = 1 AND [Col20] IS NULL) OR ([Col20] = @p64)) AND ((@p65 = 1 AND [Col21] IS NULL) OR ([Col21] = @p66)) AND ((@p67 = 1 AND [Col22] IS NULL) OR ([Col22] = @p68)))

            */

            if( !String.IsNullOrWhiteSpace( this.CommandText ) )
            {
                Boolean isLikelyAdapterUpdateStatement = this.CommandText.IndexOf( "[Col1] = @p2, [Col2] = @p3, [Col3] = @p4, [Col4] = @p5", StringComparison.Ordinal ) > -1;
                if( isLikelyAdapterUpdateStatement )
                {
                    return 1;
                }
            }

            return this.ExecuteNonQuery_Ret;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if( this.AsyncMode.AllowOld() )
            {
                Thread.Sleep( 100 );

                return this.CreateReader( this );
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

                return this.GetNonQueryResultRowCount();
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

                return this.GetNonQueryResultRowCount();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                return this.GetNonQueryResultRowCount();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                Thread.Sleep( 100 );

                return await base.ExecuteNonQueryAsync( cancellationToken );
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                await Task.Yield();

                return await Task.Run( () => this.GetNonQueryResultRowCount() );
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
