using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    public sealed class AdaSchemaMapping
    {
        /// <summary>DataColumns match in length and name order as the DataReader, no chapters</summary>
        private const int MapExactMatch = 0;

        /// <summary>DataColumns has different length, but correct name order as the DataReader, no chapters</summary>
        private const int MapDifferentSize = 1;

        /// <summary>DataColumns may have different length, but a differant name ordering as the DataReader, no chapters</summary>
        private const int MapReorderedValues = 2;

        /// <summary>DataColumns may have different length, but correct name order as the DataReader, with chapters</summary>
        private const int MapChapters = 3;

        /// <summary>DataColumns may have different length, but a differant name ordering as the DataReader, with chapters</summary>
        private const int MapChaptersReordered = 4;

        /// <summary>map xml string data to DataColumn with DataType=typeof(SqlXml)</summary>
        private const int SqlXml = 1;

        /// <summary>map xml string data to DataColumn with DataType=typeof(XmlDocument)</summary>
        private const int XmlDocument = 2;

        /// <summary>the current dataset, may be null if we are only filling a DataTable</summary>
        private readonly DataSet   dataSet;
        /// <summary>the current DataTable, should never be null</summary>
        private          DataTable dataTable;

        private readonly IAdaSchemaMappingAdapter adapter;
        private readonly AdaDataReaderContainer   dataReader;
        /// <summary>will be null if Fill without schema</summary>
        private readonly DataTable                schemaTable;
        private readonly DataTableMapping         tableMapping;

        /// <summary>unique (generated) names based from <see cref="System.Data.Common.DbDataReader.GetName(int)"/>.</summary>
        private readonly string[] fieldNames;

        private readonly object[] readerDataValues;
        /// <summary>array passed to dataRow.AddUpdate(), if needed</summary>
        private object[] mappedDataValues;

        /// <summary>index map that maps dataValues -> _mappedDataValues, if needed</summary>
        private int[] indexMap;
        /// <summary>which DataReader indexes have chapters</summary>
        private bool[] chapterMap;

        /// <summary>map which value in <see cref="readerDataValues"/> to convert to a Xml datatype, (SqlXml/XmlDocument)</summary>
        private int[] xmlMap;

        private int mappedMode; // modes as described as above
        private int mappedLength;

        private readonly LoadOption loadOption;

#if NOT_NOW
        /// <summary>Schema-mapping for a <see cref="System.Data.DataSet"/>.</summary>
        public AdaSchemaMapping(
            IAdaSchemaMappingAdapter adapter,
            DataSet                  dataset,
            AdaDataReaderContainer   dataReader,
            bool                     keyInfo,
            SchemaType               schemaType,
            string                   sourceTableName,
            bool                     gettingData,
            DataColumn               parentChapterColumn,
            object                   parentChapterValue
        )
            : this( adapter, dataset: dataset, datatable: null, dataReader, keyInfo, schemaType, sourceTableName, gettingData, parentChapterColumn, parentChapterValue )
        {
        }

        /// <summary>Schema-mapping for a single <see cref="System.Data.DataTable"/>.</summary>
        /// <param name="adapter"></param>
        /// <param name="datatable">Required.</param>
        /// <param name="dataReader"></param>
        /// <param name="keyInfo"></param>
        /// <param name="schemaType"></param>
        /// <param name="sourceTableName"></param>
        /// <param name="gettingData"></param>
        /// <param name="parentChapterColumn"></param>
        /// <param name="parentChapterValue"></param>
        public AdaSchemaMapping(
            IAdaSchemaMappingAdapter adapter,
            DataTable                datatable,
            AdaDataReaderContainer   dataReader,
            bool                     keyInfo,
            SchemaType               schemaType,
            string                   sourceTableName,
            bool                     gettingData,
            DataColumn               parentChapterColumn,
            object                   parentChapterValue
        )
            : this( adapter, dataset: null, datatable: datatable, dataReader, keyInfo, schemaType, sourceTableName, gettingData, parentChapterColumn, parentChapterValue )
        {
        }
#endif

        public AdaSchemaMapping(
            IAdaSchemaMappingAdapter adapter,
            DataSet                  dataset,
            DataTable                datatable,
            AdaDataReaderContainer   dataReader,
            bool                     keyInfo,
            SchemaType               schemaType,
            string                   sourceTableName,
            bool                     gettingData,
            DataColumn               parentChapterColumn,
            object                   parentChapterValue
        )
        {
            Debug.Assert(null != adapter, "adapter");
            Debug.Assert(null != dataReader, "dataReader");
            Debug.Assert(0 < dataReader.FieldCount, "FieldCount");
            Debug.Assert(null != dataset || null != datatable, "SchemaMapping - null dataSet");
            Debug.Assert(SchemaType.Mapped == schemaType || SchemaType.Source == schemaType, "SetupSchema - invalid schemaType");

            this.dataSet    = dataset;   // setting DataSet implies chapters are supported
            this.dataTable  = datatable; // setting only DataTable, not DataSet implies chapters are not supported
            this.adapter    = adapter;
            this.dataReader = dataReader;

            if (keyInfo)
            {
                this.schemaTable = dataReader.GetSchemaTable();
            }

            if (adapter.FillLoadOption != 0)
            {
                this.loadOption = adapter.FillLoadOption;
            }
            else if (adapter.AcceptChangesDuringFill)
            {
                this.loadOption = (LoadOption)4; // true
            }
            else
            {
                this.loadOption = (LoadOption)5; //false
            }

            MissingMappingAction mappingAction;
            MissingSchemaAction schemaAction;
            if (SchemaType.Mapped == schemaType)
            {
                mappingAction = this.adapter.MissingMappingAction;
                schemaAction = this.adapter.MissingSchemaAction;
                if (!string.IsNullOrEmpty(sourceTableName))
                { // MDAC 66034
                    this.tableMapping = this.adapter.GetTableMappingBySchemaAction(sourceTableName: sourceTableName, dataSetTableName: sourceTableName, mappingAction);
                }
                else if (null != this.dataTable)
                {
                    int index = this.adapter.IndexOfDataSetTable(this.dataTable.TableName);
                    if (-1 != index)
                    {
                        this.tableMapping = this.adapter.TableMappings[index];
                    }
                    else
                    {
                        switch (mappingAction)
                        {
                            case MissingMappingAction.Passthrough:
                                this.tableMapping = new DataTableMapping(this.dataTable.TableName, this.dataTable.TableName);
                                break;
                            case MissingMappingAction.Ignore:
                                this.tableMapping = null;
                                break;
                            case MissingMappingAction.Error:
                                throw ADP.MissingTableMappingDestination(this.dataTable.TableName);
                            default:
                                throw ADP.InvalidMissingMappingAction(mappingAction);
                        }
                    }
                }
            }
            else if (SchemaType.Source == schemaType)
            {
                mappingAction = System.Data.MissingMappingAction.Passthrough;
                schemaAction = MissingSchemaAction.Add;
                if (!string.IsNullOrEmpty(sourceTableName))
                { // MDAC 66034
                    this.tableMapping = DataTableMappingCollection.GetTableMappingBySchemaAction( tableMappings: null, sourceTable: sourceTableName, dataSetTable: sourceTableName, mappingAction);
                }
                else if (null != this.dataTable)
                {
                    int index = this.adapter.IndexOfDataSetTable(this.dataTable.TableName); // MDAC 66034
                    if (-1 != index)
                    {
                        this.tableMapping = this.adapter.TableMappings[index];
                    }
                    else
                    {
                        this.tableMapping = new DataTableMapping(this.dataTable.TableName, this.dataTable.TableName);
                    }
                }
            }
            else
            {
                throw ADP.InvalidSchemaType(schemaType);
            }

            if (null != this.tableMapping)
            {
                if (null == this.dataTable)
                {
                    this.dataTable = this.tableMapping.GetDataTableBySchemaAction(this.dataSet, schemaAction);
                }
                if (null != this.dataTable)
                {
                    this.fieldNames = GenerateFieldNames(dataReader);

                    if (null == this.schemaTable)
                    {
                        this.readerDataValues = this.SetupSchemaWithoutKeyInfo(mappingAction, schemaAction, gettingData, parentChapterColumn, parentChapterValue);
                    }
                    else
                    {
                        this.readerDataValues = this.SetupSchemaWithKeyInfo(mappingAction, schemaAction, gettingData, parentChapterColumn, parentChapterValue);
                    }
                }
                // else (null == _dataTable) which means ignore (mapped to nothing)
            }
        }

        internal AdaDataReaderContainer DataReader
        {
            get
            {
                return this.dataReader;
            }
        }

        internal DataTable DataTable
        {
            get
            {
                return this.dataTable;
            }
        }

        internal object[] DataValues
        {
            get
            {
                return this.readerDataValues;
            }
        }

        internal void ApplyToDataRow(DataRow dataRow)
        {
            DataColumnCollection columns = dataRow.Table.Columns;

            _ = this.dataReader.GetValues(this.readerDataValues); // <-- I have no idea why this call is here. I'm assuming maybe there's some side-effects induced by calling `GetValues()` in some ADO.NET providers' implementations?

            object[] mapped = this.GetMappedValues();
            bool[] readOnly = new bool[mapped.Length];
            for (int i = 0; i < readOnly.Length; ++i)
            {
                readOnly[i] = columns[i].ReadOnly;
            }

            try
            {
                try
                {
                    // allow all columns to be written to
                    for (int i = 0; i < readOnly.Length; ++i)
                    {
                        if (0 == columns[i].Expression.Length)
                        { // WebData 110773
                            columns[i].ReadOnly = false;
                        }
                    }

                    for (int i = 0; i < mapped.Length; ++i)
                    {
                        if (null != mapped[i])
                        { // MDAC 72659
                            dataRow[i] = mapped[i];
                        }
                    }
                }
                finally
                { // ReadOnly
                    // reset readonly flag on all columns
                    for (int i = 0; i < readOnly.Length; ++i)
                    {
                        if (0 == columns[i].Expression.Length)
                        { // WebData 110773
                            columns[i].ReadOnly = readOnly[i];
                        }
                    }
                }
            }
            finally
            { // FreeDataRowChapters
                if (null != this.chapterMap)
                {
                    this.FreeDataRowChapters();
                }
            }
        }

        private void MappedChapterIndex()
        { // mode 4
            int length = this.mappedLength;

            for (int i = 0; i < length; i++)
            {
                int k = this.indexMap[i];
                if (0 <= k)
                {
                    this.mappedDataValues[k] = this.readerDataValues[i]; // from reader to dataset
                    if (this.chapterMap[i])
                    {
                        this.mappedDataValues[k] = null; // InvalidCast from DataReader to AutoIncrement DataColumn
                    }
                }
            }
        }

        private void MappedChapter()
        { // mode 3
            int length = this.mappedLength;

            for (int i = 0; i < length; i++)
            {
                this.mappedDataValues[i] = this.readerDataValues[i]; // from reader to dataset
                if (this.chapterMap[i])
                {
                    this.mappedDataValues[i] = null; // InvalidCast from DataReader to AutoIncrement DataColumn
                }
            }
        }

        private void MappedIndex()
        { // mode 2
            Debug.Assert(this.mappedLength == this.indexMap.Length, "incorrect precomputed length");

            int length = this.mappedLength;
            for (int i = 0; i < length; i++)
            {
                int k = this.indexMap[i];
                if (0 <= k)
                {
                    this.mappedDataValues[k] = this.readerDataValues[i]; // from reader to dataset
                }
            }
        }

        private void MappedValues()
        { // mode 1
            Debug.Assert(this.mappedLength == Math.Min(this.readerDataValues.Length, this.mappedDataValues.Length), "incorrect precomputed length");

            int length = this.mappedLength;
            for (int i = 0; i < length; ++i)
            {
                this.mappedDataValues[i] = this.readerDataValues[i]; // from reader to dataset
            };
        }

        private object[] GetMappedValues()
        { // mode 0
            if (null != this.xmlMap)
            {
                for (int i = 0; i < this.xmlMap.Length; ++i)
                {
                    if (0 != this.xmlMap[i])
                    {
                        // get the string/SqlString xml value
                        string xml = this.readerDataValues[i] as string;
                        if( xml is null && this.readerDataValues[i] is System.Data.SqlTypes.SqlString sqlString )
                        {
                            if (!sqlString.IsNull)
                            {
                                xml = sqlString.Value;
                            }
                            else
                            {
                                switch (this.xmlMap[i])
                                {
                                    case SqlXml:
                                        // map strongly typed SqlString.Null to SqlXml.Null
                                        this.readerDataValues[i] = System.Data.SqlTypes.SqlXml.Null;
                                        break;
                                    default:
                                        this.readerDataValues[i] = DBNull.Value;
                                        break;
                                }
                            }
                        }
                        if (null != xml)
                        {
                            switch (this.xmlMap[i])
                            {
                                case SqlXml: // turn string into a SqlXml value for DataColumn
                                    System.Xml.XmlReaderSettings settings = new System.Xml.XmlReaderSettings();
                                    settings.ConformanceLevel = System.Xml.ConformanceLevel.Fragment;
                                    System.Xml.XmlReader reader = System.Xml.XmlReader.Create(new System.IO.StringReader(xml), settings, (string)null);
                                    this.readerDataValues[i] = new System.Data.SqlTypes.SqlXml(reader);
                                    break;
                                case XmlDocument: // turn string into XmlDocument value for DataColumn
                                    System.Xml.XmlDocument document = new System.Xml.XmlDocument();
                                    document.LoadXml(xml);
                                    this.readerDataValues[i] = document;
                                    break;
                            }
                            // default: let value fallthrough to DataSet which may fail with ArgumentException
                        }
                    }
                }
            }

            switch (this.mappedMode)
            {
                default:
                case MapExactMatch:
                    Debug.Assert(0 == this.mappedMode, "incorrect mappedMode");
                    Debug.Assert((null == this.chapterMap) && (null == this.indexMap) && (null == this.mappedDataValues), "incorrect MappedValues");
                    return this.readerDataValues;  // from reader to dataset
                case MapDifferentSize:
                    Debug.Assert((null == this.chapterMap) && (null == this.indexMap) && (null != this.mappedDataValues), "incorrect MappedValues");
                    this.MappedValues();
                    break;
                case MapReorderedValues:
                    Debug.Assert((null == this.chapterMap) && (null != this.indexMap) && (null != this.mappedDataValues), "incorrect MappedValues");
                    this.MappedIndex();
                    break;
                case MapChapters:
                    Debug.Assert((null != this.chapterMap) && (null == this.indexMap) && (null != this.mappedDataValues), "incorrect MappedValues");
                    this.MappedChapter();
                    break;
                case MapChaptersReordered:
                    Debug.Assert((null != this.chapterMap) && (null != this.indexMap) && (null != this.mappedDataValues), "incorrect MappedValues");
                    this.MappedChapterIndex();
                    break;
            }
            return this.mappedDataValues;
        }

        internal async Task LoadDataRowWithClearAsync( CancellationToken cancellationToken )
        {
            // for FillErrorEvent to ensure no values leftover from previous row
            for (int i = 0; i < this.readerDataValues.Length; ++i)
            {
                this.readerDataValues[i] = null;
            }

            await this.LoadDataRowAsync( cancellationToken ).ConfigureAwait(false);
        }

        internal async Task LoadDataRowAsync( CancellationToken cancellationToken )
        {
            try
            {
                _ = this.dataReader.GetValues(this.readerDataValues );
                object[] mapped = this.GetMappedValues();

                DataRow dataRow;
                switch (this.loadOption)
                {
                    case LoadOption.OverwriteChanges:
                    case LoadOption.PreserveChanges:
                    case LoadOption.Upsert:
                        dataRow = this.dataTable.LoadDataRow(mapped, this.loadOption);
                        break;
                    case (LoadOption)4: // true
                        dataRow = this.dataTable.LoadDataRow(mapped, true);
                        break;
                    case (LoadOption)5: // false
                        dataRow = this.dataTable.LoadDataRow(mapped, false);
                        break;
                    default:
                        Debug.Assert(false, "unexpected LoadOption");
                        throw ADP.InvalidLoadOption(this.loadOption);
                }

                if ((null != this.chapterMap) && (null != this.dataSet))
                {
                    _ = await this.LoadDataRowChaptersAsync( dataRow, cancellationToken ).ConfigureAwait(false); // MDAC 70772
                }
            }
            finally
            {
                if (null != this.chapterMap)
                {
                    this.FreeDataRowChapters(); // MDAC 71900
                }
            }
        }

        private void FreeDataRowChapters()
        {
            for (int i = 0; i < this.chapterMap.Length; ++i)
            {
                if (this.chapterMap[i])
                {
                    IDisposable disposable = (this.readerDataValues[i] as IDisposable);
                    if (null != disposable)
                    {
                        this.readerDataValues[i] = null;
                        disposable.Dispose();
                    }
                }
            }
        }

        internal async Task<int> LoadDataRowChaptersAsync( DataRow dataRow, CancellationToken cancellationToken )
        {
            int datarowadded = 0;

            int rowLength = this.chapterMap.Length;
            for (int i = 0; i < rowLength; ++i)
            {
                if (this.chapterMap[i])
                {
                    object readerValue = this.readerDataValues[i];
                    if ((null != readerValue) && !Convert.IsDBNull(readerValue))
                    {
                        this.readerDataValues[i] = null;

                        if( readerValue is IDataReader nestedDataReader )
                        {
                            Int32 added = await this.LoadDataRowChaptersNestedReaderAsync( dataRow, i, nestedDataReader, cancellationToken ).ConfigureAwait(false);
                            datarowadded += added;
                        }
                    }
                }
            }

            return datarowadded;
        }

        private async Task<Int32> LoadDataRowChaptersNestedReaderAsync( DataRow dataRow, Int32 i, IDataReader nestedDataReader, CancellationToken cancellationToken )
        {
            using( nestedDataReader )
            {
                if( nestedDataReader.IsClosed ) return 0;
                if( nestedDataReader is DbDataReader dbDataReader )
                {
                    Debug.Assert(null != this.dataSet, "if chapters, then Fill(DataSet,...) not Fill(DataTable,...)");

                    object parentChapterValue;
                    DataColumn parentChapterColumn;
                    if (null == this.indexMap)
                    {
                        parentChapterColumn = this.dataTable.Columns[i];
                        parentChapterValue = dataRow[parentChapterColumn];
                    }
                    else
                    {
                        parentChapterColumn = this.dataTable.Columns[this.indexMap[i]];
                        parentChapterValue = dataRow[parentChapterColumn];
                    }

                    // correct on Fill, not FillFromReader
                    string chapterTableName = this.tableMapping.SourceTable + this.fieldNames[i]; // MDAC 70908

                    AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dbDataReader, this.dataReader.ReturnProviderSpecificTypes );

                    var fillFromReaderResult = await this.adapter.FillFromReaderAsync( this.dataSet, null, chapterTableName, readerHandler, 0, 0, parentChapterColumn, parentChapterValue, cancellationToken ).ConfigureAwait(false);
                    return fillFromReaderResult;
                }
                else
                {
                    throw new InvalidOperationException( "Encountered an " + nameof(IDataReader) + " which is not a subclass of " + nameof(DbDataReader) + ". A " + nameof(DbDataReader) + " subclass is required for async operations." );
                }
            }
        }

        private static int[] CreateIndexMap(int count, int index)
        {
            int[] values = new int[count];
            for (int i = 0; i < index; ++i)
            {
                values[i] = i;
            }
            return values;
        }

        private static string[] GenerateFieldNames(AdaDataReaderContainer dataReader)
        {
            string[] fieldNames = new string[dataReader.FieldCount];
            for (int i = 0; i < fieldNames.Length; ++i)
            {
                fieldNames[i] = dataReader.GetName(i);
            }
            Utility.BuildSchemaTableInfoTableNames(fieldNames);
            return fieldNames;
        }

        private static DataColumn[] ResizeColumnArray(DataColumn[] rgcol, int len)
        {
            Debug.Assert(rgcol != null, "invalid call to ResizeArray");
            Debug.Assert(len <= rgcol.Length, "invalid len passed to ResizeArray");
            DataColumn[] tmp = new DataColumn[len];
            Array.Copy(rgcol, tmp, len);
            return tmp;
        }

        private static void AddItemToAllowRollback(ref List<object> items, object value)
        {
            if (null == items)
            {
                items = new List<object>();
            }
            items.Add(value);
        }

        private static void RollbackAddedItems(List<object> items)
        {
            if (null != items)
            {
                for (int i = items.Count - 1; 0 <= i; --i)
                {
                    // remove columns that were added now that we are failing
                    if (null != items[i])
                    {
                        DataColumn column = (items[i] as DataColumn);
                        if (null != column)
                        {
                            if (null != column.Table)
                            {
                                column.Table.Columns.Remove(column);
                            }
                        }
                        else
                        {
                            DataTable table = (items[i] as DataTable);
                            if (null != table)
                            {
                                if (null != table.DataSet)
                                {
                                    table.DataSet.Tables.Remove(table);
                                }
                            }
                        }
                    }
                }
            }
        }

        private object[] SetupSchemaWithoutKeyInfo(MissingMappingAction mappingAction, MissingSchemaAction schemaAction, bool gettingData, DataColumn parentChapterColumn, object chapterValue)
        {
            int[] columnIndexMap = null;
            bool[] chapterIndexMap = null;

            int mappingCount = 0;
            int count = this.dataReader.FieldCount;

            object[] dataValues = null;
            List<object> addedItems = null;
            try
            {
                DataColumnCollection columnCollection = this.dataTable.Columns;
                columnCollection.EnsureAdditionalCapacity_(count + (chapterValue != null ? 1 : 0));
                // We can always just create column if there are no existing column or column mappings, and the mapping action is passthrough
                bool alwaysCreateColumns = ((this.dataTable.Columns.Count == 0) && ((this.tableMapping.ColumnMappings == null) || (this.tableMapping.ColumnMappings.Count == 0)) && (mappingAction == MissingMappingAction.Passthrough));

                for (int i = 0; i < count; ++i)
                {

                    bool ischapter = false;
                    Type fieldType = this.dataReader.GetFieldType(i);

                    if (null == fieldType)
                    {
                        throw ADP.MissingDataReaderFieldType(i);
                    }

                    // if IDataReader, hierarchy exists and we will use an Int32,AutoIncrementColumn in this table
                    if (typeof(IDataReader).IsAssignableFrom(fieldType))
                    {
                        if (null == chapterIndexMap)
                        {
                            chapterIndexMap = new bool[count];
                        }
                        chapterIndexMap[i] = ischapter = true;
                        fieldType = typeof(Int32);
                    }
                    else if (typeof(System.Data.SqlTypes.SqlXml).IsAssignableFrom(fieldType))
                    {
                        if (null == this.xmlMap)
                        { // map to DataColumn with DataType=typeof(SqlXml)
                            this.xmlMap = new int[count];
                        }
                        this.xmlMap[i] = SqlXml; // track its xml data
                    }
                    else if (typeof(System.Xml.XmlReader).IsAssignableFrom(fieldType))
                    {
                        fieldType = typeof(String); // map to DataColumn with DataType=typeof(string)
                        if (null == this.xmlMap)
                        {
                            this.xmlMap = new int[count];
                        }
                        this.xmlMap[i] = XmlDocument; // track its xml data
                    }

                    DataColumn dataColumn;
                    if (alwaysCreateColumns)
                    {
                        dataColumn = DataColumnReflection.CreateDataColumnBySchemaAction_(this.fieldNames[i], this.fieldNames[i], this.dataTable, fieldType, schemaAction);
                    }
                    else
                    {
                        dataColumn = this.tableMapping.GetDataColumn(this.fieldNames[i], fieldType, this.dataTable, mappingAction, schemaAction);
                    }

                    if (null == dataColumn)
                    {
                        if (null == columnIndexMap)
                        {
                            columnIndexMap = CreateIndexMap(count, i);
                        }
                        columnIndexMap[i] = -1;
                        continue; // null means ignore (mapped to nothing)
                    }
                    else if ((null != this.xmlMap) && (0 != this.xmlMap[i]))
                    {
                        if (typeof(System.Data.SqlTypes.SqlXml) == dataColumn.DataType)
                        {
                            this.xmlMap[i] = SqlXml;
                        }
                        else if (typeof(System.Xml.XmlDocument) == dataColumn.DataType)
                        {
                            this.xmlMap[i] = XmlDocument;
                        }
                        else
                        {
                            this.xmlMap[i] = 0; // datacolumn is not a specific Xml dataType, i.e. string

                            int total = 0;
                            for (int x = 0; x < this.xmlMap.Length; ++x)
                            {
                                total += this.xmlMap[x];
                            }
                            if (0 == total)
                            { // not mapping to a specific Xml datatype, get rid of the map
                                this.xmlMap = null;
                            }
                        }
                    }

                    if (null == dataColumn.Table)
                    {
                        if (ischapter)
                        {
                            dataColumn.AllowDBNull = false;
                            dataColumn.AutoIncrement = true;
                            dataColumn.ReadOnly = true;
                        }
                        AddItemToAllowRollback(ref addedItems, dataColumn);
                        columnCollection.Add(dataColumn);
                    }
                    else if (ischapter && !dataColumn.AutoIncrement)
                    {
                        throw ADP.FillChapterAutoIncrement();
                    }


                    if (null != columnIndexMap)
                    {
                        columnIndexMap[i] = dataColumn.Ordinal;
                    }
                    else if (i != dataColumn.Ordinal)
                    {
                        columnIndexMap = CreateIndexMap(count, i);
                        columnIndexMap[i] = dataColumn.Ordinal;
                    }
                    // else i == dataColumn.Ordinal and columnIndexMap can be optimized out

                    mappingCount++;
                }
                bool addDataRelation = false;
                DataColumn chapterColumn = null;
                if (null != chapterValue)
                { // add the extra column in the child table
                    Type fieldType = chapterValue.GetType();

                    chapterColumn = this.tableMapping.GetDataColumn(this.tableMapping.SourceTable, fieldType, this.dataTable, mappingAction, schemaAction);
                    if (null != chapterColumn)
                    {

                        if (null == chapterColumn.Table)
                        {
                            AddItemToAllowRollback(ref addedItems, chapterColumn);
                            columnCollection.Add(chapterColumn);
                            addDataRelation = (null != parentChapterColumn);
                        }
                        mappingCount++;
                    }
                }

                if (0 < mappingCount)
                {
                    if ((null != this.dataSet) && (null == this.dataTable.DataSet))
                    {
                        // Allowed to throw exception if DataTable is from wrong DataSet
                        AddItemToAllowRollback(ref addedItems, this.dataTable);
                        this.dataSet.Tables.Add(this.dataTable);
                    }
                    if (gettingData)
                    {
                        if (null == columnCollection)
                        {
                            columnCollection = this.dataTable.Columns;
                        }
                        this.indexMap = columnIndexMap;
                        this.chapterMap = chapterIndexMap;
                        dataValues = this.SetupMapping(count, columnCollection, chapterColumn, chapterValue);
                    }
                    else
                    {
                        // debug only, but for retail debug ability
                        this.mappedMode = -1;
                    }
                }
                else
                {
                    this.dataTable = null;
                }

                if (addDataRelation)
                {
                    this.AddRelation(parentChapterColumn, chapterColumn);
                }

            }
            catch (Exception e)
            {
                // 
                if (ADP.IsCatchableOrSecurityExceptionType(e))
                {
                    RollbackAddedItems(addedItems);
                }
                throw;
            }
            return dataValues;
        }

        private object[] SetupSchemaWithKeyInfo(MissingMappingAction mappingAction, MissingSchemaAction schemaAction, bool gettingData, DataColumn parentChapterColumn, object chapterValue)
        {
            // must sort rows from schema table by ordinal because Jet is sorted by coumn name
            AdaDbSchemaRow[] schemaRows = AdaDbSchemaRow.GetSortedSchemaRows(this.schemaTable, this.dataReader.ReturnProviderSpecificTypes); // MDAC 60609
            Debug.Assert(null != schemaRows, "SchemaSetup - null DbSchemaRow[]");
            Debug.Assert(this.dataReader.FieldCount <= schemaRows.Length, "unexpected fewer rows in Schema than FieldCount");

            if (0 == schemaRows.Length)
            {
                this.dataTable = null;
                return null;
            }

            // Everett behavior, always add a primary key if a primary key didn't exist before
            // Whidbey behavior, same as Everett unless using LoadOption then add primary key only if no columns previously existed
            bool addPrimaryKeys = (
                (
                    (0 == this.dataTable.PrimaryKey.Length)
                    &&
                    (
                        (4 <= (int)this.loadOption)
                        ||
                        (0 == this.dataTable.Rows.Count)
                    )
                )
                ||
                (0 == this.dataTable.Columns.Count)
            ); // MDAC 67033

            DataColumn[] keys = null;
            int keyCount = 0;
            bool isPrimary = true; // assume key info (if any) is about a primary key

            string keyBaseTable = null;
            string commonBaseTable = null;

            bool keyFromMultiTable = false;
            bool commonFromMultiTable = false;

            int[] columnIndexMap = null;
            bool[] chapterIndexMap = null;

            int mappingCount = 0;

            object[] dataValues = null;
            List<object> addedItems = null;
            DataColumnCollection columnCollection = this.dataTable.Columns;
            try
            {
                for (int sortedIndex = 0; sortedIndex < schemaRows.Length; ++sortedIndex)
                {
                    AdaDbSchemaRow schemaRow = schemaRows[sortedIndex];

                    int unsortedIndex = schemaRow.UnsortedIndex; // MDAC 67050

                    bool ischapter = false;
                    Type fieldType = schemaRow.DataType;
                    if (null == fieldType)
                    {
                        fieldType = this.dataReader.GetFieldType(sortedIndex);
                    }
                    if (null == fieldType)
                    {
                        throw ADP.MissingDataReaderFieldType(sortedIndex);
                    }

                    // if IDataReader, hierarchy exists and we will use an Int32,AutoIncrementColumn in this table
                    if (typeof(IDataReader).IsAssignableFrom(fieldType))
                    {
                        if (null == chapterIndexMap)
                        {
                            chapterIndexMap = new bool[schemaRows.Length];
                        }
                        chapterIndexMap[unsortedIndex] = ischapter = true;
                        fieldType = typeof(Int32);
                    }
                    else if (typeof(System.Data.SqlTypes.SqlXml).IsAssignableFrom(fieldType))
                    {
                        if (null == this.xmlMap)
                        {
                            this.xmlMap = new int[schemaRows.Length];
                        }
                        this.xmlMap[sortedIndex] = SqlXml;
                    }
                    else if (typeof(System.Xml.XmlReader).IsAssignableFrom(fieldType))
                    {
                        fieldType = typeof(String);
                        if (null == this.xmlMap)
                        {
                            this.xmlMap = new int[schemaRows.Length];
                        }
                        this.xmlMap[sortedIndex] = XmlDocument;
                    }

                    DataColumn dataColumn = null;
                    if (!schemaRow.IsHidden)
                    {
                        dataColumn = this.tableMapping.GetDataColumn(this.fieldNames[sortedIndex], fieldType, this.dataTable, mappingAction, schemaAction);
                    }

                    string basetable = /*schemaRow.BaseServerName+schemaRow.BaseCatalogName+schemaRow.BaseSchemaName+*/ schemaRow.BaseTableName;
                    if (null == dataColumn)
                    {
                        if (null == columnIndexMap)
                        {
                            columnIndexMap = CreateIndexMap(schemaRows.Length, unsortedIndex);
                        }
                        columnIndexMap[unsortedIndex] = -1;

                        // if the column is not mapped and it is a key, then don't add any key information
                        if (schemaRow.IsKey)
                        { // MDAC 90822
#if DEBUG_OMIT
                            if (AdapterSwitches.DataSchema.TraceVerbose)
                            {
                                Debug.WriteLine("SetupSchema: partial primary key detected");
                            }
#endif
                            // if the hidden key comes from a different table - don't throw away the primary key
                            // example SELECT [T2].[ID], [T2].[ProdID], [T2].[VendorName] FROM [Vendor] AS [T2], [Prod] AS [T1] WHERE (([T1].[ProdID] = [T2].[ProdID]))
                            if (keyFromMultiTable || (schemaRow.BaseTableName == keyBaseTable))
                            { // WebData 100376
                                addPrimaryKeys = false; // don't add any future keys now
                                keys = null; // get rid of any keys we've seen
                            }
                        }
                        continue; // null means ignore (mapped to nothing)
                    }
                    else if ((null != this.xmlMap) && (0 != this.xmlMap[sortedIndex]))
                    {
                        if (typeof(System.Data.SqlTypes.SqlXml) == dataColumn.DataType)
                        {
                            this.xmlMap[sortedIndex] = SqlXml;
                        }
                        else if (typeof(System.Xml.XmlDocument) == dataColumn.DataType)
                        {
                            this.xmlMap[sortedIndex] = XmlDocument;
                        }
                        else
                        {
                            this.xmlMap[sortedIndex] = 0; // datacolumn is not a specific Xml dataType, i.e. string

                            int total = 0;
                            for (int x = 0; x < this.xmlMap.Length; ++x)
                            {
                                total += this.xmlMap[x];
                            }
                            if (0 == total)
                            { // not mapping to a specific Xml datatype, get rid of the map
                                this.xmlMap = null;
                            }
                        }
                    }

                    if (schemaRow.IsKey)
                    {
                        if (basetable != keyBaseTable)
                        {
                            if (null == keyBaseTable)
                            {
                                keyBaseTable = basetable;
                            }
                            else keyFromMultiTable = true;
                        }
                    }

                    if (ischapter)
                    {
                        if (null == dataColumn.Table)
                        {
                            dataColumn.AllowDBNull = false;
                            dataColumn.AutoIncrement = true;
                            dataColumn.ReadOnly = true;
                        }
                        else if (!dataColumn.AutoIncrement)
                        {
                            throw ADP.FillChapterAutoIncrement();
                        }
                    }
                    else
                    {// MDAC 67033
                        if (!commonFromMultiTable)
                        {
                            if ((basetable != commonBaseTable) && (!string.IsNullOrEmpty(basetable)))
                            {
                                if (null == commonBaseTable)
                                {
                                    commonBaseTable = basetable;
                                }
                                else
                                {
                                    commonFromMultiTable = true;
                                }
                            }
                        }
                        if (4 <= (int)this.loadOption)
                        {
                            if (schemaRow.IsAutoIncrement && DataColumnReflection.IsAutoIncrementType_(fieldType))
                            {
                                // 

                                dataColumn.AutoIncrement = true;

                                if (!schemaRow.AllowDBNull)
                                { // MDAC 71060
                                    dataColumn.AllowDBNull = false;
                                }
                            }

                            // setup maxLength, only for string columns since this is all the DataSet supports
                            if (fieldType == typeof(string))
                            {
                                //@devnote:  schemaRow.Size is count of characters for string columns, count of bytes otherwise
                                dataColumn.MaxLength = schemaRow.Size > 0 ? schemaRow.Size : -1;
                            }

                            if (schemaRow.IsReadOnly)
                            {
                                dataColumn.ReadOnly = true;
                            }
                            if (!schemaRow.AllowDBNull && (!schemaRow.IsReadOnly || schemaRow.IsKey))
                            { // MDAC 71060, 72252
                                dataColumn.AllowDBNull = false;
                            }

                            if (schemaRow.IsUnique && !schemaRow.IsKey && !fieldType.IsArray)
                            {
                                // note, arrays are not comparable so only mark non-arrays as unique, ie timestamp columns
                                // are unique, but not comparable
                                dataColumn.Unique = true;

                                if (!schemaRow.AllowDBNull)
                                { // MDAC 71060
                                    dataColumn.AllowDBNull = false;
                                }
                            }
                        }
                        else if (null == dataColumn.Table)
                        {
                            dataColumn.AutoIncrement = schemaRow.IsAutoIncrement;
                            dataColumn.AllowDBNull = schemaRow.AllowDBNull;
                            dataColumn.ReadOnly = schemaRow.IsReadOnly;
                            dataColumn.Unique = schemaRow.IsUnique;

                            if (fieldType == typeof(string) || (fieldType == typeof(System.Data.SqlTypes.SqlString)))
                            {
                                //@devnote:  schemaRow.Size is count of characters for string columns, count of bytes otherwise
                                dataColumn.MaxLength = schemaRow.Size;
                            }
                        }
                    }
                    if (null == dataColumn.Table)
                    {
                        if (4 > (int)this.loadOption)
                        {
                            AddAdditionalProperties(dataColumn, schemaRow.DataRow);
                        }
                        AddItemToAllowRollback(ref addedItems, dataColumn);
                        columnCollection.Add(dataColumn);
                    }

                    // The server sends us one key per table according to these rules.
                    //
                    // 1. If the table has a primary key, the server sends us this key.
                    // 2. If the table has a primary key and a unique key, it sends us the primary key
                    // 3. if the table has no primary key but has a unique key, it sends us the unique key
                    //
                    // In case 3, we will promote a unique key to a primary key IFF all the columns that compose
                    // that key are not nullable since no columns in a primary key can be null.  If one or more
                    // of the keys is nullable, then we will add a unique constraint.
                    //
                    if (addPrimaryKeys && schemaRow.IsKey)
                    { // MDAC 67033
                        if (keys == null)
                        {
                            keys = new DataColumn[schemaRows.Length];
                        }
                        keys[keyCount++] = dataColumn;
#if DEBUG_OMIT
                        if (AdapterSwitches.DataSchema.TraceVerbose)
                        {
                            Debug.WriteLine("SetupSchema: building list of " + ((isPrimary) ? "PrimaryKey" : "UniqueConstraint"));
                        }
#endif
                        // see case 3 above, we do want dataColumn.AllowDBNull not schemaRow.AllowDBNull
                        // otherwise adding PrimaryKey will change AllowDBNull to false
                        if (isPrimary && dataColumn.AllowDBNull)
                        { // MDAC 72241
#if DEBUG_OMIT
                            if (AdapterSwitches.DataSchema.TraceVerbose)
                            {
                                Debug.WriteLine("SetupSchema: changing PrimaryKey into UniqueContraint");
                            }
#endif
                            isPrimary = false;
                        }
                    }

                    if (null != columnIndexMap)
                    {
                        columnIndexMap[unsortedIndex] = dataColumn.Ordinal;
                    }
                    else if (unsortedIndex != dataColumn.Ordinal)
                    {
                        columnIndexMap = CreateIndexMap(schemaRows.Length, unsortedIndex);
                        columnIndexMap[unsortedIndex] = dataColumn.Ordinal;
                    }
                    mappingCount++;
                }

                bool addDataRelation = false;
                DataColumn chapterColumn = null;
                if (null != chapterValue)
                { // add the extra column in the child table
                    Type fieldType = chapterValue.GetType();
                    chapterColumn = this.tableMapping.GetDataColumn(this.tableMapping.SourceTable, fieldType, this.dataTable, mappingAction, schemaAction);
                    if (null != chapterColumn)
                    {

                        if (null == chapterColumn.Table)
                        {

                            chapterColumn.ReadOnly = true; // MDAC 71878
                            chapterColumn.AllowDBNull = false;

                            AddItemToAllowRollback(ref addedItems, chapterColumn);
                            columnCollection.Add(chapterColumn);
                            addDataRelation = (null != parentChapterColumn);
                        }
                        mappingCount++;
                    }
                }

                if (0 < mappingCount)
                {
                    if ((null != this.dataSet) && null == this.dataTable.DataSet)
                    {
                        AddItemToAllowRollback(ref addedItems, this.dataTable);
                        this.dataSet.Tables.Add(this.dataTable);
                    }
                    // setup the key
                    if (addPrimaryKeys && (null != keys))
                    { // MDAC 67033
                        if (keyCount < keys.Length)
                        {
                            keys = ResizeColumnArray(keys, keyCount);
                        }

                        // MDAC 66188
                        if (isPrimary)
                        {
#if DEBUG_OMIT
                            if (AdapterSwitches.DataSchema.TraceVerbose)
                            {
                                Debug.WriteLine("SetupSchema: set_PrimaryKey");
                            }
#endif
                            this.dataTable.PrimaryKey = keys;
                        }
                        else
                        {
                            UniqueConstraint unique = new UniqueConstraint("", keys);
                            ConstraintCollection constraints = this.dataTable.Constraints;
                            int constraintCount = constraints.Count;
                            for (int i = 0; i < constraintCount; ++i)
                            {
                                if (unique.Equals(constraints[i]))
                                {
#if DEBUG_OMIT
                                    if (AdapterSwitches.DataSchema.TraceVerbose)
                                    {
                                        Debug.WriteLine("SetupSchema: duplicate Contraint detected");
                                    }
#endif
                                    unique = null;
                                    break;
                                }
                            }
                            if (null != unique)
                            {
#if DEBUG_OMIT
                                if (AdapterSwitches.DataSchema.TraceVerbose)
                                {
                                    Debug.WriteLine("SetupSchema: adding new UniqueConstraint");
                                }
#endif
                                constraints.Add(unique);
                            }
                        }
                    }
                    if (!commonFromMultiTable && !string.IsNullOrEmpty(commonBaseTable) && string.IsNullOrEmpty(this.dataTable.TableName))
                    {
                        this.dataTable.TableName = commonBaseTable;
                    }
                    if (gettingData)
                    {
                        this.indexMap = columnIndexMap;
                        this.chapterMap = chapterIndexMap;
                        dataValues = this.SetupMapping(schemaRows.Length, columnCollection, chapterColumn, chapterValue);
                    }
                    else
                    {
                        // debug only, but for retail debug ability
                        this.mappedMode = -1;
                    }
                }
                else
                {
                    this.dataTable = null;
                }
                if (addDataRelation)
                {
                    this.AddRelation(parentChapterColumn, chapterColumn);
                }
            }
            catch (Exception e)
            {
                if (ADP.IsCatchableOrSecurityExceptionType(e))
                {
                    RollbackAddedItems(addedItems);
                }
                throw;
            }
            return dataValues;
        }

        private static void AddAdditionalProperties(DataColumn targetColumn, DataRow schemaRow)
        {
            DataColumnCollection columns = schemaRow.Table.Columns;
            DataColumn column;

            column = columns[SchemaTableOptionalColumn.DefaultValue];
            if (null != column)
            {
                targetColumn.DefaultValue = schemaRow[column];
            }

            column = columns[SchemaTableOptionalColumn.AutoIncrementSeed];
            if (null != column)
            {
                object value = schemaRow[column];
                if (DBNull.Value != value)
                {
                    targetColumn.AutoIncrementSeed = ((IConvertible)value).ToInt64(CultureInfo.InvariantCulture);
                }
            }

            column = columns[SchemaTableOptionalColumn.AutoIncrementStep];
            if (null != column)
            {
                object value = schemaRow[column];
                if (DBNull.Value != value)
                {
                    targetColumn.AutoIncrementStep = ((IConvertible)value).ToInt64(CultureInfo.InvariantCulture);
                }
            }

            column = columns[SchemaTableOptionalColumn.ColumnMapping];
            if (null != column)
            {
                object value = schemaRow[column];
                if (DBNull.Value != value)
                {
                    targetColumn.ColumnMapping = (MappingType)((IConvertible)value).ToInt32(CultureInfo.InvariantCulture);
                }
            }

            column = columns[SchemaTableOptionalColumn.BaseColumnNamespace];
            if (null != column)
            {
                object value = schemaRow[column];
                if (DBNull.Value != value)
                {
                    targetColumn.Namespace = ((IConvertible)value).ToString(CultureInfo.InvariantCulture);
                }
            }

            column = columns[SchemaTableOptionalColumn.Expression];
            if (null != column)
            {
                object value = schemaRow[column];
                if (DBNull.Value != value)
                {
                    targetColumn.Expression = ((IConvertible)value).ToString(CultureInfo.InvariantCulture);
                }
            }
        }

        private void AddRelation(DataColumn parentChapterColumn, DataColumn chapterColumn)
        { // MDAC 71613
            if (null != this.dataSet)
            {
                string name = /*parentChapterColumn.ColumnName + "_" +*/ chapterColumn.ColumnName; // MDAC 72815

                DataRelation relation = new DataRelation(name, new DataColumn[] { parentChapterColumn }, new DataColumn[] { chapterColumn }, false); // MDAC 71878

                int index = 1;
                string tmp = name;
                DataRelationCollection relations = this.dataSet.Relations;
                while (-1 != relations.IndexOf(tmp))
                {
                    tmp = name + index;
                    index++;
                }
                relation.RelationName = tmp;
                relations.Add(relation);
            }
        }

        private object[] SetupMapping(int count, DataColumnCollection columnCollection, DataColumn chapterColumn, object chapterValue)
        {
            object[] dataValues = new object[count];

            if (null == this.indexMap)
            {
                int mappingCount = columnCollection.Count;
                bool hasChapters = (null != this.chapterMap);
                if ((count != mappingCount) || hasChapters)
                {
                    this.mappedDataValues = new object[mappingCount];
                    if (hasChapters)
                    {

                        this.mappedMode = MapChapters;
                        this.mappedLength = count;
                    }
                    else
                    {
                        this.mappedMode = MapDifferentSize;
                        this.mappedLength = Math.Min(count, mappingCount);
                    }
                }
                else
                {
                    this.mappedMode = MapExactMatch; /* _mappedLength doesn't matter */
                }
            }
            else
            {
                this.mappedDataValues = new object[columnCollection.Count];
                this.mappedMode = ((null == this.chapterMap) ? MapReorderedValues : MapChaptersReordered);
                this.mappedLength = count;
            }
            if (null != chapterColumn)
            { // value from parent tracked into child table
                this.mappedDataValues[chapterColumn.Ordinal] = chapterValue;
            }
            return dataValues;
        }
    }
}
