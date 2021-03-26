using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract class AdaDataAdapter : Component, IUpdatingAsyncDataAdapter, IAdaSchemaMappingAdapter
    {
        private static          Int32  _objectInstanceCount;
        private static readonly Object _fillErrorEventKey = new Object();

        private LoadOption fillLoadOption; // This is initialized to zero, which is undefined on the `enum LoadOption` btw.

        private MissingMappingAction missingMappingAction = MissingMappingAction.Passthrough;
        private MissingSchemaAction  missingSchemaAction  = MissingSchemaAction.Add;

        private DataTableMappingCollection tableMappings;

        /// <summary>Normal constructor.</summary>
        protected AdaDataAdapter()
            : base()
        {
        }

        /// <summary>Clone constructor.</summary>
        protected AdaDataAdapter(AdaDataAdapter cloneFrom)
            : base()
        {
            if (cloneFrom is null) throw new ArgumentNullException(nameof(cloneFrom));

            //

            this.AcceptChangesDuringUpdate            = cloneFrom.AcceptChangesDuringUpdate;
            this.AcceptChangesDuringUpdateAfterInsert = cloneFrom.AcceptChangesDuringUpdateAfterInsert;
            this.ContinueUpdateOnError                = cloneFrom.ContinueUpdateOnError;
            this.ReturnProviderSpecificTypes          = cloneFrom.ReturnProviderSpecificTypes;
            this.AcceptChangesDuringFill              = cloneFrom.AcceptChangesDuringFill;
            this.fillLoadOption                      = cloneFrom.fillLoadOption;
            this.missingMappingAction                = cloneFrom.missingMappingAction;
            this.missingSchemaAction                 = cloneFrom.missingSchemaAction;

            if ((null != cloneFrom.tableMappings) && (0 < cloneFrom.TableMappings.Count))
            {
                DataTableMappingCollection parameters = this.TableMappings;
                foreach (object parameter in cloneFrom.TableMappings)
                {
                    if( parameter is ICloneable clonable )
                    {
                        _ = parameters.Add( clonable.Clone() );
                    }
                    else
                    {
                        _ = parameters.Add( parameter );
                    }
                }
            }
        }

        #region Properties and trivial getters

        internal int ObjectId { get; } = Interlocked.Increment(ref _objectInstanceCount);

        [DefaultValue(true)]
        public bool AcceptChangesDuringFill { get; set; } = true;

        [DefaultValue(true)]
        public bool AcceptChangesDuringUpdate { get; set; } = true;

        [DefaultValue(true)]
        public bool AcceptChangesDuringUpdateAfterInsert { get; set; } = true;

        [DefaultValue(false)]
        public bool ContinueUpdateOnError { get; set; } = false;

        [RefreshProperties(RefreshProperties.All)]
        public LoadOption FillLoadOption
        {
            get
            {
                LoadOption fillLoadOption = this.fillLoadOption;
                return ((0 != fillLoadOption) ? this.fillLoadOption : LoadOption.OverwriteChanges);
            }
            set
            {
                switch (value)
                {
                    case 0: // to allow simple resetting
                    case LoadOption.OverwriteChanges:
                    case LoadOption.PreserveChanges:
                    case LoadOption.Upsert:
                        this.fillLoadOption = value;
                        break;
                    default:
                        throw ADP.InvalidLoadOption(value);
                }
            }
        }

        [DefaultValue(MissingMappingAction.Passthrough)]
        public MissingMappingAction MissingMappingAction
        {
            get
            {
                return this.missingMappingAction;
            }
            set
            {
                switch (value)
                {
                    case MissingMappingAction.Passthrough:
                    case MissingMappingAction.Ignore:
                    case MissingMappingAction.Error:
                        this.missingMappingAction = value;
                        break;
                    default:
                        throw ADP.InvalidMissingMappingAction(value);
                }
            }
        }

        [DefaultValue(MissingSchemaAction.Add)]
        public MissingSchemaAction MissingSchemaAction
        {
            get
            {
                return this.missingSchemaAction;
            }
            set
            {
                switch (value)
                {
                    case MissingSchemaAction.Add:
                    case MissingSchemaAction.Ignore:
                    case MissingSchemaAction.Error:
                    case MissingSchemaAction.AddWithKey:
                        this.missingSchemaAction = value;
                        break;
                    default:
                        throw ADP.InvalidMissingSchemaAction(value);
                }
            }
        }

        /// <summary>When <see langword="true"/> then <see cref="DbDataReader.GetProviderSpecificFieldType"/> and <see cref="DbDataReader.GetProviderSpecificValue"/> will be used instead of <see cref="DbDataReader.GetFieldType(int)"/> and <see cref="DbDataReader.GetValue(int)"/>.</summary>
        [DefaultValue( value: false )]
        public virtual bool ReturnProviderSpecificTypes { get; set; } = false;

        [DesignerSerializationVisibility(DesignerSerializationVisibility.Content)]
        public DataTableMappingCollection TableMappings
        {
            get
            {
                if (this.tableMappings is null)
                {
                    this.tableMappings = this.CreateTableMappings() ?? new DataTableMappingCollection();
                }
                return this.tableMappings;
            }
        }

        #endregion

        #region IAdaSchemaMappingAdapter

        DataTableMapping IAdaSchemaMappingAdapter.GetTableMappingBySchemaAction( string sourceTableName, string dataSetTableName, MissingMappingAction mappingAction )
        {
            return this.GetTableMappingBySchemaAction( sourceTableName, dataSetTableName, mappingAction );
        }

        int IAdaSchemaMappingAdapter.IndexOfDataSetTable(string dataSetTable)
        {
            return this.IndexOfDataSetTable( dataSetTable );
        }

        Task<int> IAdaSchemaMappingAdapter.FillFromReaderAsync( DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken )
        {
            return this.FillFromReaderAsync( dataset, datatable, srcTable, dataReader, startRecord, maxRecords, parentChapterColumn, parentChapterValue, cancellationToken );
        }

        #endregion

        #region FillError

        [DefaultValue(false)]
        public bool HasFillErrorHandler => this.fillErrorHandlersCount > 0;

        private int fillErrorHandlersCount = 0;

        public event FillErrorEventHandler FillError
        {
            add
            {
                this.Events.AddHandler(_fillErrorEventKey, value);
                if( value != null )
                {
                    ++this.fillErrorHandlersCount; // This is crude and easily broken. I'm surprised .NET ever shipped with events-lists that cannot be introspected.
                }
            }
            remove
            {
                this.Events.RemoveHandler(_fillErrorEventKey, value);
                if( value != null && this.fillErrorHandlersCount > 0 )
                {
                    --this.fillErrorHandlersCount;
                }
            }
        }

        protected virtual void OnFillError(FillErrorEventArgs args)
        {
            FillErrorEventHandler handler = (FillErrorEventHandler)this.Events[_fillErrorEventKey];
            handler?.Invoke(this, args);
        }

        private void OnFillErrorHandler(Exception ex, DataTable dataTable, object[] dataValues)
        {
            FillErrorEventArgs fillErrorEvent = new FillErrorEventArgs( dataTable, dataValues )
            {
                Errors = ex
            };

            this.OnFillError(fillErrorEvent);

            if (!fillErrorEvent.Continue)
            {
                if (fillErrorEvent.Errors != null)
                {
                    throw fillErrorEvent.Errors;
                }

                throw ex;
            }
        }

        #endregion

        protected virtual DataTableMappingCollection CreateTableMappings()
        {
            return new DataTableMappingCollection();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            { // release mananged objects
                this.tableMappings = null;
            }
            // release unmanaged objects

            base.Dispose(disposing); // notify base classes
        }

        #region FillSchemaAsync

        public abstract Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken );

        protected virtual Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, DbDataReader dataReader, CancellationToken cancellationToken )
        {
            return AsyncDataReaderMethods.FillSchemaAsync( adapter: this, this.ReturnProviderSpecificTypes, dataSet: dataSet, schemaType, srcTable, dataReader, cancellationToken );
        }

        protected virtual Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, DbDataReader dataReader, CancellationToken cancellationToken )
        {
            return AsyncDataReaderMethods.FillSchemaAsync( adapter: this, this.ReturnProviderSpecificTypes, dataTable: dataTable, schemaType, dataReader, cancellationToken );
        }

        #endregion

        #region FillAsync

        public abstract Task<int> FillAsync( DataSet dataSet, CancellationToken cancellationToken = default );

        protected virtual async Task<int> FillAsync( DataSet dataSet, string srcTable, DbDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            if (null == dataSet) throw ADP.FillRequires("dataSet");
            if (string.IsNullOrEmpty(srcTable)) throw ADP.FillRequiresSourceTableName("srcTable");
            if (null == dataReader) throw ADP.FillRequires("dataReader");
            if (startRecord < 0) throw ADP.InvalidStartRecord("startRecord", startRecord);
            if (maxRecords < 0) throw ADP.InvalidMaxRecords("maxRecords", maxRecords);

            //

            if (dataReader.IsClosed)
            {
                return 0;
            }

            // user must Close/Dispose of the dataReader
            AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dataReader, useProviderSpecificDataReader: this.ReturnProviderSpecificTypes );
            return await this.FillFromReaderAsync( dataSet, null, srcTable, readerHandler, startRecord, maxRecords, null, null, cancellationToken ).ConfigureAwait(false);
        }

        protected virtual async Task<int> FillAsync(DataTable dataTable, DbDataReader dataReader, CancellationToken cancellationToken)
        {
            DataTable[] dataTables = new DataTable[] { dataTable };

            return await this.FillAsync( dataTables, dataReader, startRecord: 0, maxRecords: 0, cancellationToken ).ConfigureAwait(false);
        }

        protected virtual Task<int> FillAsync( DataTable[] dataTables, DbDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            Action<Exception, DataTable, Object[]> onFillError = this.GetCurrentFillErrorHandler();

            return AsyncDataReaderMethods.FillAsync( onFillError, this, this.ReturnProviderSpecificTypes, dataTables, dataReader, startRecord, maxRecords, cancellationToken );
        }

        internal Task<int> FillFromReaderAsync( DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken )
        {
            Action<Exception, DataTable, Object[]> onFillError = this.GetCurrentFillErrorHandler();

            return AsyncDataReaderMethods.FillFromReaderAsync( onFillError, this, dataset, datatable, srcTable, dataReader, startRecord, maxRecords, parentChapterColumn, parentChapterValue, cancellationToken );
        }

        public Task<int> FillLoadDataRowChunkAsync( AdaSchemaMapping mapping, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            Action<Exception, DataTable, Object[]> onFillError2 = this.GetCurrentFillErrorHandler();

            return AsyncDataReaderMethods.FillLoadDataRowChunkAsync( onFillError2, mapping, startRecord, maxRecords, cancellationToken );
        }

        private AdaSchemaMapping FillMappingInternal(DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {
            return AsyncDataReaderMethods.FillMappingInternal( this, dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
        }

        private AdaSchemaMapping FillMapping(DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {
            Action<Exception, DataTable, Object[]> onFillError = this.GetCurrentFillErrorHandler();

            return AsyncDataReaderMethods.FillMapping( onFillError, this, dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
        }

        private async Task<bool> FillNextResultAsync( AdaDataReaderContainer dataReader, CancellationToken cancellationToken )
        {
            Action<Exception, DataTable, Object[]> onFillError = this.GetCurrentFillErrorHandler();

            return await AsyncDataReaderMethods.FillNextResultAsync( onFillError, dataReader, cancellationToken ).ConfigureAwait(false);
        }

        private Action<Exception, DataTable, Object[]> GetCurrentFillErrorHandler()
        {
            if( this.HasFillErrorHandler )
            {
                return this.OnFillErrorHandler;
            }

            return null;
        }

        #endregion

        [EditorBrowsable(EditorBrowsableState.Advanced)]
        public abstract IDataParameter[] GetFillParameters();

        internal DataTableMapping GetTableMappingBySchemaAction(string sourceTableName, string dataSetTableName, MissingMappingAction mappingAction)
        {
            return DataTableMappingCollection.GetTableMappingBySchemaAction(this.tableMappings, sourceTableName, dataSetTableName, mappingAction);
        }

        internal int IndexOfDataSetTable(string dataSetTable)
        {
            if (null != this.tableMappings)
            {
                return this.TableMappings.IndexOfDataSetTable( dataSetTable );
            }
            return -1;
        }

        public abstract Task<int> UpdateAsync(DataSet dataSet, CancellationToken cancellationToken );

        // used by FillSchema which returns an array of datatables added to the dataset
        private static DataTable[] AddDataTableToArray(DataTable[] tables, DataTable newTable)
        {
            return AsyncDataReaderMethods.AddDataTableToArray( tables, newTable );
        }

        // dynamically generate source table names
        private static string GetSourceTableName(string srcTable, int index)
        {
            //if ((null != srcTable) && (0 <= index) && (index < srcTable.Length)) {
            if (0 == index)
            {
                return srcTable; //[index];
            }
            return srcTable + index.ToString(CultureInfo.InvariantCulture);
        }

        #region IDataAdapter

        int IDataAdapter.Fill(DataSet dataSet)
        {
            throw new NotImplementedException( "Non-async operations are not currently supported." );
        }

        DataTable[] IDataAdapter.FillSchema(DataSet dataSet, SchemaType schemaType)
        {
            throw new NotImplementedException( "Non-async operations are not currently supported." );
        }

        int IDataAdapter.Update(DataSet dataSet)
        {
            throw new NotImplementedException( "Non-async operations are not currently supported." );
        }

        ITableMappingCollection IDataAdapter.TableMappings => this.TableMappings;

        #endregion
    }
}
