using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> :
        AsyncDbDataAdapter<TDbCommand>,
        IAsyncDbDataAdapter

        where TDbDataAdapter : DbDataAdapter
        where TDbConnection  : DbConnection
        where TDbCommand     : DbCommand
        where TDbDataReader  : DbDataReader
    {
        public static implicit operator TDbDataAdapter( ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> self )
        {
            return self?.Subject;
        }

        /// <summary></summary>
        /// <param name="subject"></param>
        /// <param name="batchingAdapter">Can be null. This feature is not required.</param>
        protected ProxyDbDataAdapter( TDbDataAdapter subject, IBatchingAdapter batchingAdapter )
            : base( adapter: subject ?? throw new ArgumentNullException(nameof(subject)) ) // The `adapter` clone ctor copies state over. Which is fine as that sets initial state.
        {
            this.Subject         = subject;
            this.BatchingAdapter = batchingAdapter;

            this.SetDbDataAdapter_SelectCommand( subject.SelectCommand );
            this.SetDbDataAdapter_InsertCommand( subject.InsertCommand );
            this.SetDbDataAdapter_UpdateCommand( subject.UpdateCommand );
            this.SetDbDataAdapter_DeleteCommand( subject.DeleteCommand );

            this.Subject.FillError += this.OnSubjectFillError;
        }

        protected TDbDataAdapter Subject { get; }

        [DefaultValue(null)]
        protected IBatchingAdapter BatchingAdapter { get; }

        #region TDbCommands

        // There's a problem: `DbDataAdapter`'s ctor calls the `SelectCommand_set` (and insert/update/delete) before control passes to the subclass ctor.
        // But this class can't set `this.Subject` until then...
        // So here's an ugly hack:

//      private TDbCommand selectCommandSetByCtor;
//      private TDbCommand insertCommandSetByCtor;
//      private TDbCommand deleteCommandSetByCtor;
//      private TDbCommand updateCommandSetByCtor;

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private static void CheckForStackOverflow()
        {
            // https://stackoverflow.com/questions/15337469/debugging-unit-tests-that-fail-due-to-a-stackoverflow-exception
        }

        #region IDbDataAdapter shenanigans

        // `DbDataAdapter.set_SelectCommand` calls into its vtable for `IDbDataAdapter.set_SelectCommand` which it also implements and stores in its own field.

        protected void SetDbDataAdapter_SelectCommand( DbCommand cmd )
        {
            DbDataAdapter baseSelf = this;
            IDbDataAdapter baseSelfAsInterface = baseSelf;
            baseSelfAsInterface.SelectCommand = cmd;
        }

        protected void SetDbDataAdapter_InsertCommand( DbCommand cmd )
        {
            DbDataAdapter baseSelf = this;
            IDbDataAdapter baseSelfAsInterface = baseSelf;
            baseSelfAsInterface.InsertCommand = cmd;
        }

        protected void SetDbDataAdapter_UpdateCommand( DbCommand cmd )
        {
            DbDataAdapter baseSelf = this;
            IDbDataAdapter baseSelfAsInterface = baseSelf;
            baseSelfAsInterface.UpdateCommand = cmd;
        }

        protected void SetDbDataAdapter_DeleteCommand( DbCommand cmd )
        {
            DbDataAdapter baseSelf = this;
            IDbDataAdapter baseSelfAsInterface = baseSelf;
            baseSelfAsInterface.DeleteCommand = cmd;
        }

        #endregion

        public override TDbCommand SelectCommand
        {
            get
            {
#if DEBUG
                RuntimeHelpers.EnsureSufficientExecutionStack(); // https://stackoverflow.com/questions/5491654/insufficientexecutionstackexception
#endif
                return (TDbCommand)this.Subject.SelectCommand;
            }
            set
            {
#if DEBUG
                RuntimeHelpers.EnsureSufficientExecutionStack(); // https://stackoverflow.com/questions/5491654/insufficientexecutionstackexception
#endif
                if( this.Subject is null )
                {
//                  if( value != null ) this.selectCommandSetByCtor = value;
                }
                else
                {
                    this.Subject.SelectCommand = value;
                    this.SetDbDataAdapter_SelectCommand( value );
                }
            }
        }

        public new TDbCommand InsertCommand
        {
            get
            {
#if DEBUG
                RuntimeHelpers.EnsureSufficientExecutionStack();
#endif
                return (TDbCommand)base.InsertCommand;
            }
            set
            {
#if DEBUG
                RuntimeHelpers.EnsureSufficientExecutionStack();
#endif
                if( this.Subject is null )
                {
//                  if( value != null ) this.insertCommandSetByCtor = value;
                }
                else
                {
                    this.Subject.InsertCommand = value;
                    this.SetDbDataAdapter_InsertCommand( value );
                }
            }
        }

        public new TDbCommand DeleteCommand
        {
            get
            {
#if DEBUG
                RuntimeHelpers.EnsureSufficientExecutionStack();
#endif
                 return (TDbCommand)base.DeleteCommand;
            }
            set
            {
#if DEBUG
                RuntimeHelpers.EnsureSufficientExecutionStack();
#endif
                if( this.Subject is null )
                {
//                  if( value != null ) this.deleteCommandSetByCtor = value;
                }
                else
                {
                    this.Subject.DeleteCommand = value;
                    this.SetDbDataAdapter_DeleteCommand( value );
                }
            }
        }

        public new TDbCommand UpdateCommand
        {
            get
            {
#if DEBUG
                RuntimeHelpers.EnsureSufficientExecutionStack();
#endif
                return (TDbCommand)base.UpdateCommand;
            }
            set
            {
#if DEBUG
                RuntimeHelpers.EnsureSufficientExecutionStack();
#endif
                if( this.Subject is null )
                {
//                  if( value != null ) this.updateCommandSetByCtor = value;
                }
                else
                {
                    this.Subject.UpdateCommand = value;
                    this.SetDbDataAdapter_UpdateCommand( value );
                }
            }
        }

        #endregion

        #region Connections

        protected static TDbConnection GetConnection( TDbCommand command )
        {
            if (command is null) throw new ArgumentNullException(nameof(command));
            
            DbConnection dbc = command.Connection;
            if (dbc is TDbConnection c)
            {
                return c;
            }
            else if (dbc is null)
            {
                throw new InvalidOperationException( "Command.Connection must not be null." );
            }
            else
            {
                throw new InvalidOperationException( "Expected Command.Connection to be of type " + typeof(TDbCommand).FullName + ", but encountered " + dbc.GetType().FullName + " instead." );
            }
        }

        protected static async Task<ConnectionState> QuietOpenAsync( TDbConnection connection, CancellationToken cancellationToken )
        {
            if (connection is null) throw new ArgumentNullException(nameof(connection));

            ConnectionState originalState = connection.State;
            if (originalState == ConnectionState.Closed)
            {
                await connection.OpenAsync( cancellationToken ).ConfigureAwait(false);
            }

            return originalState;
        }

        protected static void QuietClose( TDbConnection connection, ConnectionState originalState )
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

        protected static async Task<TDbDataReader> ExecuteReaderAsync( TDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken )
        {
            DbDataReader reader = await command.ExecuteReaderAsync( behavior, cancellationToken ).ConfigureAwait(false);
            return (TDbDataReader)reader;
        }

        #endregion
    }
}
