using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.SqlClient
{
    public sealed class AdaSqlDataAdapter : AdaDbDataAdapter
    {
        private static readonly object EventRowUpdated  = new object();
        private static readonly object EventRowUpdating = new object();
        
        private ISqlCommandSet _commandSet;
        private int _updateBatchSize = 1;

        public AdaSqlDataAdapter( IBatchingAdapter batchingAdapter )
            : base( batchingAdapter )
        {
            GC.SuppressFinalize(this);
        }

        public AdaSqlDataAdapter( IBatchingAdapter batchingAdapter, SqlCommand selectCommand)
            : this( batchingAdapter )
        {
            this.SelectCommand = selectCommand;
        }

        public AdaSqlDataAdapter( IBatchingAdapter batchingAdapter, string selectCommandText, string selectConnectionString)
            : this( batchingAdapter )
        {
            SqlConnection connection = new SqlConnection(selectConnectionString);
            this.SelectCommand = new SqlCommand(selectCommandText, connection);
        }

        public AdaSqlDataAdapter( IBatchingAdapter batchingAdapter, string selectCommandText, SqlConnection selectConnection)
            : this( batchingAdapter )
        {
            this.SelectCommand = new SqlCommand(selectCommandText, selectConnection);
        }

        /// <summary>Clone constructor.</summary>
        private AdaSqlDataAdapter(AdaSqlDataAdapter cloneFrom)
            : base(cloneFrom)
        {
            GC.SuppressFinalize(this);
        }

        #region Properties

        [DefaultValue(null)]
        public new SqlCommand DeleteCommand
        {
            get { return (SqlCommand)base.DeleteCommand; }
            set { base.DeleteCommand = value; }
        }

        [DefaultValue(null)]
        public new SqlCommand InsertCommand
        {
            get { return (SqlCommand)base.InsertCommand; }
            set { base.InsertCommand = value; }
        }

        [DefaultValue(null)]
        public new SqlCommand SelectCommand
        {
            get { return (SqlCommand)base.SelectCommand; }
            set { base.SelectCommand = value; }
        }

        [DefaultValue(null)]
        public new SqlCommand UpdateCommand
        {
            get { return (SqlCommand)base.UpdateCommand; }
            set { base.UpdateCommand = value; }
        }

        #endregion

        /// <summary>This implementation always returns a new <see cref="AdaSqlDataAdapter"/> instance.</summary>
        public override Object Clone()
        {
            return new AdaSqlDataAdapter( cloneFrom: this );
        }

        public override int UpdateBatchSize
        {
            get
            {
                return this._updateBatchSize;
            }
            set
            {
                if (0 > value) // i.e. `value < 0`
                { // WebData 98157
                    throw new ArgumentOutOfRangeException(paramName: nameof(value), actualValue: value, message: nameof(this.UpdateBatchSize) + " value must be >= 0." );
                }
                this._updateBatchSize = value;
            }
        }

        public event SqlRowUpdatedEventHandler RowUpdated
        {
            add
            {
                this.Events.AddHandler(EventRowUpdated, value);
            }
            remove
            {
                this.Events.RemoveHandler(EventRowUpdated, value);
            }
        }

        public event SqlRowUpdatingEventHandler RowUpdating
        {
            add
            {
                SqlRowUpdatingEventHandler handler = (SqlRowUpdatingEventHandler)this.Events[EventRowUpdating];

                // MDAC 58177, 64513
                // prevent someone from registering two different command builders on the adapter by
                // silently removing the old one
                if ((null != handler) && (value.Target is DbCommandBuilder))
                {
                    SqlRowUpdatingEventHandler d = (SqlRowUpdatingEventHandler)Utility.FindBuilder(handler);
                    if (null != d)
                    {
                        this.Events.RemoveHandler(EventRowUpdating, d);
                    }
                }
                this.Events.AddHandler(EventRowUpdating, value);
            }
            remove
            {
                this.Events.RemoveHandler(EventRowUpdating, value);
            }
        }

        protected override int AddToBatch(DbCommand command)
        {
            int commandIdentifier = this._commandSet.CommandCount;
            this._commandSet.Append((SqlCommand)command);
            return commandIdentifier;
        }

        protected override void ClearBatch()
        {
            this._commandSet.Clear();
        }

        protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            return new SqlRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
        }

        protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            return new SqlRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
        }

        protected override Task<int> ExecuteBatchAsync(CancellationToken cancellationToken)
        {
            Debug.Assert(null != this._commandSet && (0 < this._commandSet.CommandCount), "no commands");
            // TODO:    Bid.CorrelationTrace("<sc.SqlDataAdapter.ExecuteBatch|Info|Correlation> ObjectID%d#, ActivityID %ls\n", ObjectID);
            return this._commandSet.ExecuteNonQueryAsync( cancellationToken );
        }

        protected override IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex)
        {
            Debug.Assert(commandIdentifier < this._commandSet.CommandCount, "commandIdentifier out of range");
            Debug.Assert(parameterIndex < this._commandSet.GetParameterCount(commandIdentifier), "parameter out of range");
            IDataParameter parameter = this._commandSet.GetParameter(commandIdentifier, parameterIndex);
            return parameter;
        }

        protected override bool GetBatchedRecordsAffected(int commandIdentifier, out int recordsAffected, out Exception error)
        {
            Debug.Assert(commandIdentifier < this._commandSet.CommandCount, "commandIdentifier out of range");
            return this._commandSet.GetBatchedAffected(commandIdentifier, out recordsAffected, out error);
        }

        protected override void InitializeBatching()
        {
            this._commandSet = SqlCommandSetFactory.CreateInstance();
            SqlCommand command = this.SelectCommand;
            if (null == command)
            {
                command = this.InsertCommand;
                if (null == command)
                {
                    command = this.UpdateCommand;
                    if (null == command)
                    {
                        command = this.DeleteCommand;
                    }
                }
            }
            if (command != null)
            {
                this._commandSet.Connection     = command.Connection;
                this._commandSet.Transaction    = command.Transaction;
                this._commandSet.CommandTimeout = command.CommandTimeout;
            }
        }

        protected override void OnRowUpdated(RowUpdatedEventArgs value)
        {
            SqlRowUpdatedEventHandler handler = (SqlRowUpdatedEventHandler)this.Events[EventRowUpdated];
            if ((null != handler) && (value is SqlRowUpdatedEventArgs))
            {
                handler(this, (SqlRowUpdatedEventArgs)value);
            }
            base.OnRowUpdated(value);
        }

        protected override void OnRowUpdating(RowUpdatingEventArgs value)
        {
            SqlRowUpdatingEventHandler handler = (SqlRowUpdatingEventHandler)this.Events[EventRowUpdating];
            if ((null != handler) && (value is SqlRowUpdatingEventArgs))
            {
                handler(this, (SqlRowUpdatingEventArgs)value);
            }
            base.OnRowUpdating(value);
        }

        protected override void TerminateBatching()
        {
            if (null != this._commandSet)
            {
                this._commandSet.Dispose();
                this._commandSet = null;
            }
        }
    }
}
