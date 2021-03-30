using System;
using System.Data;
using System.Data.Common;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    /// <summary>The purpose of this type is just to add strongly-typed GetCommand wrapper methods.</summary>
    public abstract class DbCommandBuilder<TDbCommand> : DbCommandBuilder, IDbCommandBuilder
        where TDbCommand : DbCommand
    {
        protected DbCommandBuilder()
            : base()
        {
        }

        public new TDbCommand GetDeleteCommand( Boolean useColumnsForParameterNames ) => (TDbCommand)base.GetDeleteCommand( useColumnsForParameterNames );
        public new TDbCommand GetDeleteCommand()                                      => (TDbCommand)base.GetDeleteCommand();
        public new TDbCommand GetInsertCommand( Boolean useColumnsForParameterNames ) => (TDbCommand)base.GetInsertCommand( useColumnsForParameterNames );
        public new TDbCommand GetInsertCommand()                                      => (TDbCommand)base.GetInsertCommand();
        public new TDbCommand GetUpdateCommand( Boolean useColumnsForParameterNames ) => (TDbCommand)base.GetUpdateCommand( useColumnsForParameterNames );
        public new TDbCommand GetUpdateCommand()                                      => (TDbCommand)base.GetUpdateCommand();
    }

    /// <summary>Extends <see cref="DbCommandBuilder"/> with support for asynchronous operations by pre-emptively asynchronously loading data to avoid unexpected synchronous database IO calls</summary>
    public sealed class ProxyDbCommandBuilder</*TDbCommandBuilder,*/TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> : DbCommandBuilder<TDbCommand>, IAsyncDbCommandBuilder
//      where TDbCommandBuilder: DbCommandBuilder
        where TDbDataAdapter   : DbDataAdapter
//      where TDbDataAdapter   : ProxyDbDataAdapter<>
        where TDbConnection    : DbConnection
        where TDbCommand       : DbCommand
        where TDbDataReader    : DbDataReader
    {
//      private readonly ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> adaper;
        private readonly DataTable selectCommandResultsSchema;

        public ProxyDbCommandBuilder(
//          TDbCommandBuilder subject,
            DbCommandBuilder subject,
//          ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> proxyDataAdapter,
            DataTable selectCommandResultsSchema
        )
            : base()
        {
            this.Subject                    = subject                    ?? throw new ArgumentNullException(nameof(subject));
//          this.adaper                     = proxyDataAdapter           ?? throw new ArgumentNullException(nameof(proxyDataAdapter));
            this.selectCommandResultsSchema = selectCommandResultsSchema ?? throw new ArgumentNullException(nameof(selectCommandResultsSchema));

            base.DataAdapter = this.Subject.DataAdapter;
        }

//      public TDbCommandBuilder Subject { get; }
        public DbCommandBuilder Subject { get; }

        public new ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> DataAdapter { get; }

        IAsyncDbDataAdapter IAsyncDbCommandBuilder.DataAdapter => this.DataAdapter;

        /// <summary>This method is called by <see cref="DbCommandBuilder"/>'s non-virtual <c>BuildCache</c> method. This overrided implementation returns a known DataTable loaded beforehand to avoid non-async IO.</summary>
        protected override DataTable GetSchemaTable(DbCommand sourceCommand)
        {
            return this.selectCommandResultsSchema;
        }

        //

        private struct _ApplyParameterInfo { }

        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
            ReflectedAction<DbCommandBuilder,_ApplyParameterInfo,DbParameter,DataRow,StatementType,Boolean>.Invoke( instance: this.Subject, parameter, row, statementType, whereClause );
        }

        private struct _GetParameterName { }

        protected override String GetParameterName(int parameterOrdinal)
        {
            return ReflectedFunc<DbCommandBuilder,_GetParameterName,Int32,String>.Invoke( instance: this.Subject, parameterOrdinal );
        }

        protected override String GetParameterName(string parameterName)
        {
            return ReflectedFunc<DbCommandBuilder,_GetParameterName,String,String>.Invoke( instance: this.Subject, parameterName );
        }

        private struct _GetParameterPlaceholder { }

        protected override String GetParameterPlaceholder(int parameterOrdinal)
        {
            return ReflectedFunc<DbCommandBuilder,_GetParameterPlaceholder,Int32,String>.Invoke( instance: this.Subject, parameterOrdinal );
        }

        private struct _SetRowUpdatingHandler { }

        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            ReflectedAction<DbCommandBuilder,_SetRowUpdatingHandler,DbDataAdapter>.Invoke( instance: this.Subject, adapter );
        }
    }
}

namespace AsyncDataAdapter.Internal.CommandBuilderFaffing
{
    public sealed class SubversiveDataAdapter : DbDataAdapter
    {
        public SubversiveDataAdapter()
        {

        }
    }

    /// <summary>Ensures that <see cref="DbCommandBuilder"/>'s <c>BuildCache</c> method doesn't perform any IO.</summary>
    public sealed class NoopDbConnection : DbConnection
    {
        private readonly DataTable dataSourceInformationSchemaTable;

        public NoopDbConnection( DataTable dataSourceInformationSchemaTable )
        {
            this.dataSourceInformationSchemaTable = dataSourceInformationSchemaTable ?? throw new ArgumentNullException(nameof(dataSourceInformationSchemaTable));
        }

        public override ConnectionState State => ConnectionState.Open;

        public override void Open()
        {
            // NOOP.
        }

        public override void Close()
        {
            // NOOP.
        }

        public override DataTable GetSchema(string collectionName)
        {
            if( collectionName == DbMetaDataCollectionNames.DataSourceInformation )
            {
                return this.dataSourceInformationSchemaTable;
            }
            else
            {
                return base.GetSchema( collectionName );
            }
        }

        #region DbConnection abstract members

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }

        protected override DbCommand CreateDbCommand()
        {
            throw new NotSupportedException();
        }

        public override string ConnectionString { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override string Database => throw new NotImplementedException();

        public override string DataSource => throw new NotImplementedException();

        public override string ServerVersion => throw new NotImplementedException();

        #endregion
    }
}
