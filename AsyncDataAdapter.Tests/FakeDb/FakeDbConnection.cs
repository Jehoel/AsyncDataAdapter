using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Tests.FakeDb
{
    // hmmm, need to subclass `DbProviderFactory` too?

    public class FakeDbConnection : DbConnection
    {
        public FakeDbConnection( AsyncMode asyncMode, Int32 openDelayMS = 10, Int32 executeDelayMS = 10, Int32 readDelayMS = 10 )
        {
            this.AsyncMode    = asyncMode;

            this.OpenDelay    = TimeSpan.FromMilliseconds( openDelayMS );
            this.ExecuteDelay = TimeSpan.FromMilliseconds( executeDelayMS );
            this.ReadDelay    = TimeSpan.FromMilliseconds( readDelayMS );
        }

        protected override DbProviderFactory DbProviderFactory => FakeDbProviderFactory.Instance;

        public FakeDbProviderFactory Factory => FakeDbProviderFactory.Instance;

        #region Data Source

        public override void ChangeDatabase(String databaseName)
        {
            this.DatabaseValue = databaseName;
        }

#if NET50
        public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        {
            return base.ChangeDatabaseAsync(databaseName, cancellationToken);
        }
#endif

        public override String ConnectionString { get; set; }

        public override String Database => this.DatabaseValue;

        public String DatabaseValue { get; set; } = "FakeDatabase";

        public override String DataSource => "FakeDataSource";

        public override String ServerVersion => "1.2.3";

        #endregion

        #region Test data

        public AsyncMode AsyncMode { get; set; }

        public TimeSpan OpenDelay    { get; set; }
        public TimeSpan ExecuteDelay { get; set; }
        public TimeSpan ReadDelay    { get; set; }

        #endregion

        #region CreateCommand

        protected override DbCommand CreateDbCommand() => this.CreateCommand( testTables: null );

        public new FakeDbCommand CreateCommand() => this.CreateCommand( testTables: null );

        public FakeDbCommand CreateCommand( List<TestTable> testTables )
        {
            return this.Factory.CreateCommand( this, testTables: testTables, executeDelay: this.ExecuteDelay, readDelay: this.ReadDelay );
        }

        #endregion

        #region GetSchema

        private static DataTable Create_DataSourceInformation_SchemaTable()
        {
            DataTable schemaTable = new DataTable()
            {
                Columns =
                {
                    new DataColumn( columnName: DbMetaDataColumnNames.ParameterNamePattern  , dataType: typeof(String) ),
                    new DataColumn( columnName: DbMetaDataColumnNames.ParameterMarkerFormat , dataType: typeof(String) ),
                    new DataColumn( columnName: DbMetaDataColumnNames.ParameterNameMaxLength, dataType: typeof(Int32)  )
                }
            };

            _ = schemaTable.Rows.Add( @"^[\p{Lo}\p{Lu}\p{Ll}\p{Lm}_@#][\p{Lo}\p{Lu}\p{Ll}\p{Lm}\p{Nd}\uff3f_@#\$]*(?=\s+|$)", @"{0}", 128 );

            return schemaTable;
        }

        #if NET50
        public virtual Task<DataTable> GetSchemaAsync( CancellationToken cancellationToken = default )
        {
            return base.GetSchemaAsync( cancellationToken );
        }

        public virtual Task<DataTable> GetSchemaAsync( string collectionName, CancellationToken cancellationToken = default )
        {
            return base.GetSchemaAsync( collectionName, cancellationToken );
        }

        public virtual Task<DataTable> GetSchemaAsync( string collectionName, string?[] restrictionValues, CancellationToken cancellationToken = default )
        {
            return base.GetSchemaAsync( collectionName, restrictionValues, cancellationToken );
        }

        #endif

        public override DataTable GetSchema()
        {
            if( this.AsyncMode.AllowOld() )
            {
                return base.GetSchema();
            }
            else
            {
                throw new NotSupportedException( "AllowSync is false." );
            }
        }

        public override DataTable GetSchema(string collectionName)
        {
            if( this.AsyncMode.AllowOld() )
            {
                if( collectionName == DbMetaDataCollectionNames.DataSourceInformation )
                {
                    return Create_DataSourceInformation_SchemaTable();
                }
                else
                {
                    return base.GetSchema(collectionName);
                }
            }
            else
            {
                throw new NotSupportedException( "AllowSync is false." );
            }
        }

        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            if( this.AsyncMode.AllowOld() )
            {
                return base.GetSchema(collectionName, restrictionValues);
            }
            else
            {
                throw new NotSupportedException( "AllowSync is false." );
            }
        }

        #endregion

        #region Open + Close and Connection State

        public ConnectionState StateValue { get; set; } = ConnectionState.Closed;

        public override ConnectionState State => this.StateValue;

        protected override void OnStateChange(StateChangeEventArgs stateChange)
        {
            base.OnStateChange(stateChange);
        }

        public override void Open()
        {
            if( this.AsyncMode.AllowOld() )
            {
                Thread.Sleep( 100 );

                this.StateValue = ConnectionState.Open; 
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

                this.StateValue = ConnectionState.Open; 
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BlockAsync ) )
            {
                Thread.Sleep( 100 );

                this.StateValue = ConnectionState.Open; 
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.BaseAsync ) )
            {
                await base.OpenAsync();
            }
            else if( this.AsyncMode.HasFlag( AsyncMode.RunAsync ) )
            {
                await Task.Run( () => { this.StateValue = ConnectionState.Open; } );
            }
            else
            {
                throw new NotSupportedException( "AllowAsync is false." );
            }
        }

        public override void Close()
        {
            this.StateValue = ConnectionState.Closed;
        }

#if NET50
        public override Task CloseAsync()
        {
            return base.CloseAsync();
        }
#endif

        #endregion

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
    }
}
