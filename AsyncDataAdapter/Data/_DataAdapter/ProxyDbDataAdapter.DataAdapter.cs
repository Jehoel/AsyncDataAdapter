using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader>
    {
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

        public new MissingSchemaAction        MissingSchemaAction       { get => this.Subject.MissingSchemaAction      ; set { this.Subject.MissingSchemaAction       = value; base.MissingSchemaAction       = value; } }
        public new MissingMappingAction       MissingMappingAction      { get => this.Subject.MissingMappingAction     ; set { this.Subject.MissingMappingAction      = value; base.MissingMappingAction      = value; } }
        public new LoadOption                 FillLoadOption            { get => this.Subject.FillLoadOption           ; set { this.Subject.FillLoadOption            = value; base.FillLoadOption            = value; } }
        public new Boolean                    ContinueUpdateOnError     { get => this.Subject.ContinueUpdateOnError    ; set { this.Subject.ContinueUpdateOnError     = value; base.ContinueUpdateOnError     = value; } }
        public new Boolean                    AcceptChangesDuringUpdate { get => this.Subject.AcceptChangesDuringUpdate; set { this.Subject.AcceptChangesDuringUpdate = value; base.AcceptChangesDuringUpdate = value; } }
        public new Boolean                    AcceptChangesDuringFill   { get => this.Subject.AcceptChangesDuringFill  ; set { this.Subject.AcceptChangesDuringFill   = value; base.AcceptChangesDuringFill   = value; } }
        public new DataTableMappingCollection TableMappings             { get => this.Subject.TableMappings; }

        public override Boolean ReturnProviderSpecificTypes
        {
            get => this.Subject.ReturnProviderSpecificTypes;
            set
            {
                this.Subject.ReturnProviderSpecificTypes = value;
                base.ReturnProviderSpecificTypes = value;
            }
        }

        public override Int32            Fill( DataSet dataSet )                              => this.Subject.Fill( dataSet );
        public override DataTable[]      FillSchema( DataSet dataSet, SchemaType schemaType ) => this.Subject.FillSchema( dataSet, schemaType );
        public override IDataParameter[] GetFillParameters()                                  => this.Subject.GetFillParameters();
        public override Boolean          ShouldSerializeAcceptChangesDuringFill()             => this.Subject.ShouldSerializeAcceptChangesDuringFill();
        public override Boolean          ShouldSerializeFillLoadOption()                      => this.Subject.ShouldSerializeFillLoadOption();
        public override Int32            Update( DataSet dataSet )                            => this.Subject.Update( dataSet );

        // Because all of the virtual entrypoints above are overridden and forwarded to the subject, none of the protected-virtual methods should be invoked...
        // UPDATE: ...with the exception of the following:
        // * CreateTableMappings (via `DataAdapter.get_TableMappings`)

        [Obsolete]
        protected override DataAdapter                CloneInternals()                                                                                      => throw new InvalidOperationException( "protected virtual method " + nameof(this.CloneInternals)               + "() should never be invoked." );
        protected override Int32                      Fill( DataTable dataTable, IDataReader dataReader )                                                   => throw new InvalidOperationException( "protected virtual method " + nameof(this.Fill)                         + "(DataTable dataTable, IDataReader dataReader) should never be invoked." );
        protected override Int32                      Fill( DataSet dataSet, String srcTable, IDataReader dataReader, Int32 startRecord, Int32 maxRecords ) => throw new InvalidOperationException( "protected virtual method " + nameof(this.Fill)                         + "(DataSet dataSet, String srcTable, IDataReader dataReader, Int32 startRecord, Int32 maxRecords) should never be invoked." );
        protected override DataTable                  FillSchema( DataTable dataTable, SchemaType schemaType, IDataReader dataReader )                      => throw new InvalidOperationException( "protected virtual method " + nameof(this.FillSchema)                   + "(DataTable dataTable, SchemaType schemaType, IDataReader dataReader) should never be invoked." );
        protected override DataTable[]                FillSchema( DataSet dataSet, SchemaType schemaType, String srcTable, IDataReader dataReader )         => throw new InvalidOperationException( "protected virtual method " + nameof(this.FillSchema)                   + "(DataSet dataSet, SchemaType schemaType, String srcTable, IDataReader dataReader) should never be invoked." );
        protected override Boolean                    ShouldSerializeTableMappings()                                                                        => throw new InvalidOperationException( "protected virtual method " + nameof(this.ShouldSerializeTableMappings) + "() should never be invoked." );

        private struct _CreateTableMappings { }
        protected override DataTableMappingCollection CreateTableMappings()
        {
            return Internal.ReflectedFunc<DataAdapter,_CreateTableMappings,DataTableMappingCollection>.Invoke( this.Subject );
        }

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
                this.Subject.FillError -= this.OnSubjectFillError;
                this.Subject.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
