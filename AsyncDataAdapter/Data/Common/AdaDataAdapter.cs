
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
    public abstract class AdaDataAdapter : Component, IUpdatingAsyncDataAdapter
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

        protected bool HasTableMappings()
        {
            return ( this.tableMappings?.Count ?? 0 ) > 0;
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

        protected virtual async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, DbDataReader dataReader, CancellationToken cancellationToken )
        {
            if (null == dataSet) throw new ArgumentNullException(nameof(dataSet));
            if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType)) throw ADP.InvalidSchemaType(schemaType);
            if (string.IsNullOrEmpty(srcTable)) throw ADP.FillSchemaRequiresSourceTableName("srcTable");
            if ((null == dataReader) || dataReader.IsClosed) throw ADP.FillRequires("dataReader");

            // user must Close/Dispose of the dataReader
            return await this.FillSchemaFromReaderAsync( dataSet, singleDataTable: null, schemaType, srcTable, dataReader, cancellationToken ).ConfigureAwait(false);
        }

        protected virtual async Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, DbDataReader dataReader, CancellationToken cancellationToken )
        {
            if (null == dataTable) throw new ArgumentNullException(nameof(dataTable));
            if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType)) throw ADP.InvalidSchemaType(schemaType);
            if ((null == dataReader) || dataReader.IsClosed)throw ADP.FillRequires("dataReader");

            // user must Close/Dispose of the dataReader
            // user will have to call NextResult to access remaining results
            DataTable[] singleTable = await this.FillSchemaFromReaderAsync( dataset: null, singleDataTable: dataTable, schemaType, srcTable: null, dataReader, cancellationToken ).ConfigureAwait(false);
            if( singleTable is DataTable[] arr && arr.Length == 1 ) return singleTable[0];
            return null;
        }

        internal async Task<DataTable[]> FillSchemaFromReaderAsync( DataSet dataset, DataTable singleDataTable, SchemaType schemaType, string srcTable, DbDataReader dataReader, CancellationToken cancellationToken )
        {
            DataTable[] dataTables = null;
            int schemaCount = 0;
            do
            {
                AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dataReader, useProviderSpecificDataReader: this.ReturnProviderSpecificTypes );
                if (0 >= readerHandler.FieldCount)
                {
                    continue;
                }

                string sourceTableName = null;
                if (null != dataset)
                {
                    sourceTableName = AdaDataAdapter.GetSourceTableName(srcTable, schemaCount);
                    schemaCount++; // don't increment if no SchemaTable ( a non-row returning result )
                }

                AdaSchemaMapping mapping = new AdaSchemaMapping( adapter: this, dataset, singleDataTable, dataReader: readerHandler, keyInfo: true, schemaType, sourceTableName, gettingData: false, parentChapterColumn: null, parentChapterValue: null );

                if (singleDataTable != null)
                {
                    // do not read remaining results in single DataTable case
                    return new DataTable[] { mapping.DataTable };
                }
                else if (null != mapping.DataTable)
                {
                    if (null == dataTables)
                    {
                        dataTables = new DataTable[1] { mapping.DataTable };
                    }
                    else
                    {
                        dataTables = AdaDataAdapter.AddDataTableToArray(dataTables, mapping.DataTable);
                    }
                }
            }
            while ( await dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false) ); // FillSchema does not capture errors for FillError event

            if( dataTables is null && singleDataTable is null )
            {
                return Array.Empty<DataTable>();
            }
            else
            {
                return dataTables;
            }
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

        protected virtual async Task<int> FillAsync( DataTable[] dataTables, DbDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            if (dataTables is null) throw new ArgumentNullException(paramName: nameof(dataTables));
            if (0 == dataTables.Length) throw new ArgumentException(string.Format("Argument is empty: {0}", nameof(dataTables)), paramName: nameof(dataTables));

            {
                if (null == dataTables[0])
                {
                    throw ADP.FillRequires("dataTable");
                }
                if (null == dataReader)
                {
                    throw ADP.FillRequires("dataReader");
                }
                if ((1 < dataTables.Length) && ((0 != startRecord) || (0 != maxRecords)))
                {
                    throw new NotSupportedException(); // FillChildren is not supported with FillPage
                }

                int result = 0;
                bool enforceContraints = false;
                DataSet commonDataSet = dataTables[0].DataSet;
                try
                {
                    if (null != commonDataSet)
                    {
                        enforceContraints = commonDataSet.EnforceConstraints;
                        commonDataSet.EnforceConstraints = false;
                    }
                    for (int i = 0; i < dataTables.Length; ++i)
                    {
                        Debug.Assert(null != dataTables[i], "null DataTable Fill");

                        if (dataReader.IsClosed)
                        {
                            break;
                        }

                        AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dataReader, useProviderSpecificDataReader: this.ReturnProviderSpecificTypes );
                        if (readerHandler.FieldCount <= 0)
                        {
                            if (i == 0)
                            {
                                bool lastFillNextResult;
                                do
                                {
                                    lastFillNextResult = await this.FillNextResultAsync( readerHandler, cancellationToken ).ConfigureAwait(false);
                                }
                                while (lastFillNextResult && readerHandler.FieldCount <= 0);
                                
                                if (!lastFillNextResult)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                continue;
                            }
                        }
                       
                        if ((0 < i) && !await this.FillNextResultAsync( readerHandler, cancellationToken ).ConfigureAwait(false))
                        {
                            break;
                        }
                        // user must Close/Dispose of the dataReader
                        // user will have to call NextResult to access remaining results
                        int count = await this.FillFromReaderAsync( null, dataTables[i], null, readerHandler, startRecord, maxRecords, null, null, cancellationToken ).ConfigureAwait(false);
                        if (0 == i)
                        {
                            result = count;
                        }
                    }
                }
                catch (ConstraintException)
                {
                    enforceContraints = false;
                    throw;
                }
                finally
                {
                    if (enforceContraints)
                    {
                        commonDataSet.EnforceConstraints = true;
                    }
                }

                return result;
            }
        }

        internal async Task<int> FillFromReaderAsync( DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken )
        {
            int rowsAddedToDataSet = 0;
            int schemaCount = 0;
            do
            {
                if (0 >= dataReader.FieldCount)
                {
                    continue; // loop to next result
                }

                AdaSchemaMapping mapping = this.FillMapping( dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
                schemaCount++; // don't increment if no SchemaTable ( a non-row returning result )

                if (null == mapping)
                {
                    continue; // loop to next result
                }
                if (null == mapping.DataValues)
                {
                    continue; // loop to next result
                }
                if (null == mapping.DataTable)
                {
                    continue; // loop to next result
                }

                mapping.DataTable.BeginLoadData();

                try
                {
                    // startRecord and maxRecords only apply to the first resultset
                    if ((1 == schemaCount) && ((0 < startRecord) || (0 < maxRecords)))
                    {
                        rowsAddedToDataSet = await this.FillLoadDataRowChunkAsync( mapping, startRecord, maxRecords, cancellationToken ).ConfigureAwait(false);
                    }
                    else
                    {
                        int count = await this.FillLoadDataRowAsync( mapping, cancellationToken ).ConfigureAwait(false);

                        if (1 == schemaCount)
                        {
                            // only return LoadDataRow count for first resultset
                            // not secondary or chaptered results
                            rowsAddedToDataSet = count;
                        }
                    }
                }
                finally
                {
                    mapping.DataTable.EndLoadData();
                }
                if (null != datatable)
                {
                    break; // do not read remaining results in single DataTable case
                }
            }
            while (await this.FillNextResultAsync( dataReader, cancellationToken ).ConfigureAwait(false));

            return rowsAddedToDataSet;
        }

        private async Task<int> FillLoadDataRowChunkAsync( AdaSchemaMapping mapping, int startRecord, int maxRecords, CancellationToken cancellationToken )
        {
            AdaDataReaderContainer dataReader = mapping.DataReader;

            while (0 < startRecord)
            {
                if (!await dataReader.ReadAsync( cancellationToken ).ConfigureAwait(false))
                {
                    // there are no more rows on first resultset
                    return 0;
                }
                --startRecord;
            }

            int rowsAddedToDataSet = 0;
            if (0 < maxRecords)
            {
                while ((rowsAddedToDataSet < maxRecords) && await dataReader.ReadAsync( cancellationToken ).ConfigureAwait(false))
                {
                    if (this.HasFillErrorHandler)
                    {
                        try
                        {
                            await mapping.LoadDataRowWithClearAsync( cancellationToken ).ConfigureAwait(false);
                            rowsAddedToDataSet++;
                        }
                        catch (Exception e)
                        {
                            // 
                            if (!ADP.IsCatchableExceptionType(e))
                            {
                                throw;
                            }
                            this.OnFillErrorHandler(e, mapping.DataTable, mapping.DataValues);
                        }
                    }
                    else
                    {
                        await mapping.LoadDataRowAsync( cancellationToken ).ConfigureAwait(false);
                        rowsAddedToDataSet++;
                    }
                }
                // skip remaining rows of the first resultset
            }
            else
            {
                rowsAddedToDataSet = await this.FillLoadDataRowAsync( mapping, cancellationToken ).ConfigureAwait(false);
            }
            return rowsAddedToDataSet;
        }

        private async Task<int> FillLoadDataRowAsync( AdaSchemaMapping mapping, CancellationToken cancellationToken )
        {
            int rowsAddedToDataSet = 0;
            AdaDataReaderContainer dataReader = mapping.DataReader;
            if (this.HasFillErrorHandler)
            {
                while (await dataReader.ReadAsync( cancellationToken ).ConfigureAwait(false))
                { // read remaining rows of first and subsequent resultsets
                    try
                    {
                        // only try-catch if a FillErrorEventHandler is registered so that
                        // in the default case we get the full callstack from users
                        await mapping.LoadDataRowWithClearAsync( cancellationToken ).ConfigureAwait(false);
                        rowsAddedToDataSet++;
                    }
                    catch (Exception e)
                    {
                        // 
                        if (!ADP.IsCatchableExceptionType(e))
                        {
                            throw;
                        }
                        this.OnFillErrorHandler(e, mapping.DataTable, mapping.DataValues);
                    }
                }
            }
            else
            {
                while (await dataReader.ReadAsync( cancellationToken ).ConfigureAwait(false))
                {
                    // read remaining rows of first and subsequent resultset
                    await mapping.LoadDataRowAsync( cancellationToken ).ConfigureAwait(false);
                    rowsAddedToDataSet++;
                }
            }
            return rowsAddedToDataSet;
        }

        private AdaSchemaMapping FillMappingInternal(DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {
            bool withKeyInfo = (MissingSchemaAction.AddWithKey == this.MissingSchemaAction);
            string tmp = null;
            if (dataset != null)
            {
                tmp = AdaDataAdapter.GetSourceTableName(srcTable, schemaCount);
            }

            return new AdaSchemaMapping( this, dataset, datatable, dataReader, withKeyInfo, SchemaType.Mapped, tmp, true, parentChapterColumn, parentChapterValue );
        }

        private AdaSchemaMapping FillMapping(DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {
            AdaSchemaMapping mapping = null;
            if (this.HasFillErrorHandler)
            {
                try
                {
                    // only try-catch if a FillErrorEventHandler is registered so that
                    // in the default case we get the full callstack from users
                    mapping = this.FillMappingInternal( dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
                }
                catch (Exception e)
                {
                    if (!ADP.IsCatchableExceptionType(e))
                    {
                        throw;
                    }
                    this.OnFillErrorHandler(e, null, null);
                }
            }
            else
            {
                mapping = this.FillMappingInternal( dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
            }
            return mapping;
        }

        private async Task<bool> FillNextResultAsync( AdaDataReaderContainer dataReader, CancellationToken cancellationToken )
        {
            bool result = true;
            if (this.HasFillErrorHandler)
            {
                try
                {
                    // only try-catch if a FillErrorEventHandler is registered so that
                    // in the default case we get the full callstack from users
                    result = await dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (!ADP.IsCatchableExceptionType(e))
                    {
                        throw;
                    }
                    this.OnFillErrorHandler(e, null, null);
                }
            }
            else
            {
                result = await dataReader.NextResultAsync( cancellationToken ).ConfigureAwait(false);
            }

            return result;
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
            for (int i = 0; i < tables.Length; ++i)
            {
                // search for duplicates:
                if (Object.ReferenceEquals( tables[i], newTable ))
                {
                    return tables; // duplicate found
                }
            }
            DataTable[] newTables = new DataTable[tables.Length + 1]; // add unique data table
            for (int i = 0; i < tables.Length; ++i)
            {
                newTables[i] = tables[i];
            }
            newTables[tables.Length] = newTable;
            return newTables;
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
