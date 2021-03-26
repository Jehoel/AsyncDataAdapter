using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    // NOTE: This type is completely unconcerned with anything "DbData..." related - including IDbDataAdapter, DbDataAdapter, and so on.

    public abstract partial class ProxyDataAdapter : DataAdapter, IDataAdapter
    {
        protected readonly DataAdapter subject;

        protected ProxyDataAdapter( DataAdapter subject )
            // The `from` clone ctor copies state over. Which is fine as that sets initial state.
            : base( from: subject ?? throw new ArgumentNullException(nameof(subject)) )
        {
            this.subject = subject ?? throw new ArgumentNullException(nameof(subject));

            this.subject.FillError += this.OnSubjectFillError;
        }

        #region Reimplement IDataAdapter (needed so the interface's mutable features are correctly implemented)

        MissingMappingAction IDataAdapter.MissingMappingAction
        {
            get => this.MissingMappingAction;
            set => this.MissingMappingAction = value;
        }

        MissingSchemaAction IDataAdapter.MissingSchemaAction
        {
            get => this.MissingSchemaAction;
            set => this.MissingSchemaAction = value;
        }

        ITableMappingCollection IDataAdapter.TableMappings => this.TableMappings;
        
        Int32 IDataAdapter.Fill( DataSet dataSet ) => this.Fill( dataSet );

        DataTable[] IDataAdapter.FillSchema( DataSet dataSet, SchemaType schemaType ) => this.FillSchema( dataSet, schemaType );

        IDataParameter[] IDataAdapter.GetFillParameters() => this.GetFillParameters();

        Int32 IDataAdapter.Update( DataSet dataSet ) => this.Update( dataSet );

        #endregion

        #region DataAdapter virtuals

        public new MissingSchemaAction        MissingSchemaAction       { get => this.subject.MissingSchemaAction      ; set { this.subject.MissingSchemaAction       = value; base.MissingSchemaAction       = value; } }
        public new MissingMappingAction       MissingMappingAction      { get => this.subject.MissingMappingAction     ; set { this.subject.MissingMappingAction      = value; base.MissingMappingAction      = value; } }
        public new LoadOption                 FillLoadOption            { get => this.subject.FillLoadOption           ; set { this.subject.FillLoadOption            = value; base.FillLoadOption            = value; } }
        public new Boolean                    ContinueUpdateOnError     { get => this.subject.ContinueUpdateOnError    ; set { this.subject.ContinueUpdateOnError     = value; base.ContinueUpdateOnError     = value; } }
        public new Boolean                    AcceptChangesDuringUpdate { get => this.subject.AcceptChangesDuringUpdate; set { this.subject.AcceptChangesDuringUpdate = value; base.AcceptChangesDuringUpdate = value; } }
        public new Boolean                    AcceptChangesDuringFill   { get => this.subject.AcceptChangesDuringFill  ; set { this.subject.AcceptChangesDuringFill   = value; base.AcceptChangesDuringFill   = value; } }
        public new DataTableMappingCollection TableMappings             { get => this.subject.TableMappings; }

        public override Boolean ReturnProviderSpecificTypes
        {
            get => this.subject.ReturnProviderSpecificTypes;
            set
            {
                this.subject.ReturnProviderSpecificTypes = value;
                base.ReturnProviderSpecificTypes = value;
            }
        }

        public override Int32            Fill( DataSet dataSet )                              => this.subject.Fill( dataSet );
        public override DataTable[]      FillSchema( DataSet dataSet, SchemaType schemaType ) => this.subject.FillSchema( dataSet, schemaType );
        public override IDataParameter[] GetFillParameters()                                  => this.subject.GetFillParameters();
        public override Boolean          ShouldSerializeAcceptChangesDuringFill()             => this.subject.ShouldSerializeAcceptChangesDuringFill();
        public override Boolean          ShouldSerializeFillLoadOption()                      => this.subject.ShouldSerializeFillLoadOption();
        public override Int32            Update( DataSet dataSet )                            => this.subject.Update( dataSet );

        // Because all of the virtual entrypoints above are overridden and forwarded to the subject, none of the protected-virtual methods should be invoked:

        [Obsolete]
        protected override DataAdapter                CloneInternals()                                                                                      => throw new InvalidOperationException( "protected virtual method " + nameof(this.CloneInternals)               + "() should never be invoked." );
        protected override DataTableMappingCollection CreateTableMappings()                                                                                 => throw new InvalidOperationException( "protected virtual method " + nameof(this.CloneInternals)               + "() should never be invoked." );
        protected override Int32                      Fill( DataTable[] dataTables, IDataReader dataReader, Int32 startRecord, Int32 maxRecords )           => throw new InvalidOperationException( "protected virtual method " + nameof(this.Fill)                         + "(DataTable[] dataTables, IDataReader dataReader, Int32 startRecord, Int32 maxRecords) should never be invoked." );
        protected override Int32                      Fill( DataTable dataTable, IDataReader dataReader )                                                   => throw new InvalidOperationException( "protected virtual method " + nameof(this.Fill)                         + "(DataTable dataTable, IDataReader dataReader) should never be invoked." );
        protected override Int32                      Fill( DataSet dataSet, String srcTable, IDataReader dataReader, Int32 startRecord, Int32 maxRecords ) => throw new InvalidOperationException( "protected virtual method " + nameof(this.Fill)                         + "(DataSet dataSet, String srcTable, IDataReader dataReader, Int32 startRecord, Int32 maxRecords) should never be invoked." );
        protected override DataTable                  FillSchema( DataTable dataTable, SchemaType schemaType, IDataReader dataReader )                      => throw new InvalidOperationException( "protected virtual method " + nameof(this.FillSchema)                   + "(DataTable dataTable, SchemaType schemaType, IDataReader dataReader) should never be invoked." );
        protected override DataTable[]                FillSchema( DataSet dataSet, SchemaType schemaType, String srcTable, IDataReader dataReader )         => throw new InvalidOperationException( "protected virtual method " + nameof(this.FillSchema)                   + "(DataSet dataSet, SchemaType schemaType, String srcTable, IDataReader dataReader) should never be invoked." );
        protected override Boolean                    ShouldSerializeTableMappings()                                                                        => throw new InvalidOperationException( "protected virtual method " + nameof(this.ShouldSerializeTableMappings) + "() should never be invoked." );

        private void OnSubjectFillError(Object sender, FillErrorEventArgs e)
        {
            this.OnFillError( e );
        }

        protected override void OnFillError( FillErrorEventArgs value )
        {
            base.OnFillError(value);
        }

        #endregion

        protected override void Dispose(bool disposing)
        {
            if( disposing )
            {
                this.subject.FillError -= this.OnSubjectFillError;
                this.subject.Dispose();
            }

            base.Dispose(disposing);
        }

        #region IAsyncDataReader

        public abstract Task<int> FillAsync(DataSet dataSet, CancellationToken cancellationToken = default);

        public abstract Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken = default);

        #endregion

        #region IUpdatingAsyncDataAdapter

        public abstract Task<int> UpdateAsync(DataSet dataSet, CancellationToken cancellationToken = default);

        #endregion
    }
}
