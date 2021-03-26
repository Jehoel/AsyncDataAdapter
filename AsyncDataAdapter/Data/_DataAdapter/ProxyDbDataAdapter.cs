using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    // NOTE: This type is completely unconcerned with anything "DbData..." related - including IDbDataAdapter, DbDataAdapter, and so on.

    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> : /*ProxyDataAdapter*/ DbDataAdapter, IDbDataAdapter
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

            this.Subject.FillError += this.OnSubjectFillError;
        }

        protected TDbDataAdapter Subject { get; }

        [DefaultValue(null)]
        protected IBatchingAdapter BatchingAdapter { get; }

        #region TDbCommands

        public new TDbCommand SelectCommand
        {
            get => (TDbCommand)this.Subject.SelectCommand;
            set
            {
                this.Subject.SelectCommand = value;
                base.SelectCommand = value;
            }
        }

        public new TDbCommand InsertCommand
        {
            get => (TDbCommand)base.InsertCommand;
            set
            {
                this.Subject.InsertCommand = value;
                base.InsertCommand = value;
            }
        }

        public new TDbCommand DeleteCommand
        {
            get => (TDbCommand)base.DeleteCommand;
            set
            {
                this.Subject.DeleteCommand = value;
                base.DeleteCommand = value;
            }
        }

        public new TDbCommand UpdateCommand
        {
            get => (TDbCommand)base.UpdateCommand;
            set
            {
                this.Subject.UpdateCommand = value;
                base.UpdateCommand = value;
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
