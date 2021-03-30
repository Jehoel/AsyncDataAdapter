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

        // DataColumns match in length and name order as the DataReader, no chapters
        private const int MapExactMatch = 0;

        // DataColumns has different length, but correct name order as the DataReader, no chapters
        private const int MapDifferentSize = 1;

        // DataColumns may have different length, but a differant name ordering as the DataReader, no chapters
        private const int MapReorderedValues = 2;

        // DataColumns may have different length, but correct name order as the DataReader, with chapters
        private const int MapChapters = 3;

        // DataColumns may have different length, but a differant name ordering as the DataReader, with chapters
        private const int MapChaptersReordered = 4;

        // map xml string data to DataColumn with DataType=typeof(SqlXml)
        private const int SqlXml = 1;

        // map xml string data to DataColumn with DataType=typeof(XmlDocument)
        private const int XmlDocument = 2;

        private readonly DataSet _dataSet; // the current dataset, may be null if we are only filling a DataTable
        private DataTable _dataTable; // the current DataTable, should never be null

        private readonly IAdaSchemaMappingAdapter _adapter;
        private readonly AdaDataReaderContainer _dataReader;
        private readonly DataTable _schemaTable;  // will be null if Fill without schema
        private readonly DataTableMapping _tableMapping;

        // unique (generated) names based from DataReader.GetName(i)
        private readonly string[] _fieldNames;

        private readonly object[] _readerDataValues;
        private object[] _mappedDataValues; // array passed to dataRow.AddUpdate(), if needed

        private int[] _indexMap;     // index map that maps dataValues -> _mappedDataValues, if needed
        private bool[] _chapterMap;  // which DataReader indexes have chapters

        private int[] _xmlMap; // map which value in _readerDataValues to convert to a Xml datatype, (SqlXml/XmlDocument)

        private int _mappedMode; // modes as described as above
        private int _mappedLength;

        private readonly LoadOption _loadOption;

        internal AdaSchemaMapping(IAdaSchemaMappingAdapter adapter, DataSet dataset, DataTable datatable, AdaDataReaderContainer dataReader, bool keyInfo,
                                    SchemaType schemaType, string sourceTableName, bool gettingData,
                                    DataColumn parentChapterColumn, object parentChapterValue)
        {
            Debug.Assert(null != adapter, "adapter");
            Debug.Assert(null != dataReader, "dataReader");
            Debug.Assert(0 < dataReader.FieldCount, "FieldCount");
            Debug.Assert(null != dataset || null != datatable, "SchemaMapping - null dataSet");
            Debug.Assert(SchemaType.Mapped == schemaType || SchemaType.Source == schemaType, "SetupSchema - invalid schemaType");

            this._dataSet = dataset;     // setting DataSet implies chapters are supported
            this._dataTable = datatable; // setting only DataTable, not DataSet implies chapters are not supported
            this._adapter = adapter;
            this._dataReader = dataReader;

            if (keyInfo)
            {
                this._schemaTable = dataReader.GetSchemaTable();
            }

            if (adapter.FillLoadOption != 0)
            {
                this._loadOption = adapter.FillLoadOption;
            }
            else if (adapter.AcceptChangesDuringFill)
            {
                this._loadOption = (LoadOption)4; // true
            }
            else
            {
                this._loadOption = (LoadOption)5; //false
            }

            MissingMappingAction mappingAction;
            MissingSchemaAction schemaAction;
            if (SchemaType.Mapped == schemaType)
            {
                mappingAction = this._adapter.MissingMappingAction;
                schemaAction = this._adapter.MissingSchemaAction;
                if (!string.IsNullOrEmpty(sourceTableName))
                { // MDAC 66034
                    this._tableMapping = this._adapter.GetTableMappingBySchemaAction(sourceTableName, sourceTableName, mappingAction);
                }
                else if (null != this._dataTable)
                {
                    int index = this._adapter.IndexOfDataSetTable(this._dataTable.TableName);
                    if (-1 != index)
                    {
                        this._tableMapping = this._adapter.TableMappings[index];
                    }
                    else
                    {
                        switch (mappingAction)
                        {
                            case MissingMappingAction.Passthrough:
                                this._tableMapping = new DataTableMapping(this._dataTable.TableName, this._dataTable.TableName);
                                break;
                            case MissingMappingAction.Ignore:
                                this._tableMapping = null;
                                break;
                            case MissingMappingAction.Error:
                                throw ADP.MissingTableMappingDestination(this._dataTable.TableName);
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
                    this._tableMapping = DataTableMappingCollection.GetTableMappingBySchemaAction(null, sourceTableName, sourceTableName, mappingAction);
                }
                else if (null != this._dataTable)
                {
                    int index = this._adapter.IndexOfDataSetTable(this._dataTable.TableName); // MDAC 66034
                    if (-1 != index)
                    {
                        this._tableMapping = this._adapter.TableMappings[index];
                    }
                    else
                    {
                        this._tableMapping = new DataTableMapping(this._dataTable.TableName, this._dataTable.TableName);
                    }
                }
            }
            else
            {
                throw ADP.InvalidSchemaType(schemaType);
            }

            if (null != this._tableMapping)
            {
                if (null == this._dataTable)
                {
                    this._dataTable = this._tableMapping.GetDataTableBySchemaAction(this._dataSet, schemaAction);
                }
                if (null != this._dataTable)
                {
                    this._fieldNames = GenerateFieldNames(dataReader);

                    if (null == this._schemaTable)
                    {
                        this._readerDataValues = this.SetupSchemaWithoutKeyInfo(mappingAction, schemaAction, gettingData, parentChapterColumn, parentChapterValue);
                    }
                    else
                    {
                        this._readerDataValues = this.SetupSchemaWithKeyInfo(mappingAction, schemaAction, gettingData, parentChapterColumn, parentChapterValue);
                    }
                }
                // else (null == _dataTable) which means ignore (mapped to nothing)
            }
        }

        internal AdaDataReaderContainer DataReader
        {
            get
            {
                return this._dataReader;
            }
        }

        internal DataTable DataTable
        {
            get
            {
                return this._dataTable;
            }
        }

        internal object[] DataValues
        {
            get
            {
                return this._readerDataValues;
            }
        }

        internal void ApplyToDataRow(DataRow dataRow)
        {
            DataColumnCollection columns = dataRow.Table.Columns;
            this._dataReader.GetValues(this._readerDataValues);

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
                if (null != this._chapterMap)
                {
                    this.FreeDataRowChapters();
                }
            }
        }

        private void MappedChapterIndex()
        { // mode 4
            int length = this._mappedLength;

            for (int i = 0; i < length; i++)
            {
                int k = this._indexMap[i];
                if (0 <= k)
                {
                    this._mappedDataValues[k] = this._readerDataValues[i]; // from reader to dataset
                    if (this._chapterMap[i])
                    {
                        this._mappedDataValues[k] = null; // InvalidCast from DataReader to AutoIncrement DataColumn
                    }
                }
            }
        }

        private void MappedChapter()
        { // mode 3
            int length = this._mappedLength;

            for (int i = 0; i < length; i++)
            {
                this._mappedDataValues[i] = this._readerDataValues[i]; // from reader to dataset
                if (this._chapterMap[i])
                {
                    this._mappedDataValues[i] = null; // InvalidCast from DataReader to AutoIncrement DataColumn
                }
            }
        }

        private void MappedIndex()
        { // mode 2
            Debug.Assert(this._mappedLength == this._indexMap.Length, "incorrect precomputed length");

            int length = this._mappedLength;
            for (int i = 0; i < length; i++)
            {
                int k = this._indexMap[i];
                if (0 <= k)
                {
                    this._mappedDataValues[k] = this._readerDataValues[i]; // from reader to dataset
                }
            }
        }

        private void MappedValues()
        { // mode 1
            Debug.Assert(this._mappedLength == Math.Min(this._readerDataValues.Length, this._mappedDataValues.Length), "incorrect precomputed length");

            int length = this._mappedLength;
            for (int i = 0; i < length; ++i)
            {
                this._mappedDataValues[i] = this._readerDataValues[i]; // from reader to dataset
            };
        }

        private object[] GetMappedValues()
        { // mode 0
            if (null != this._xmlMap)
            {
                for (int i = 0; i < this._xmlMap.Length; ++i)
                {
                    if (0 != this._xmlMap[i])
                    {
                        // get the string/SqlString xml value
                        string xml = this._readerDataValues[i] as string;
                        if( xml is null && this._readerDataValues[i] is System.Data.SqlTypes.SqlString sqlString )
                        {
                            if (!sqlString.IsNull)
                            {
                                xml = sqlString.Value;
                            }
                            else
                            {
                                switch (this._xmlMap[i])
                                {
                                    case SqlXml:
                                        // map strongly typed SqlString.Null to SqlXml.Null
                                        this._readerDataValues[i] = System.Data.SqlTypes.SqlXml.Null;
                                        break;
                                    default:
                                        this._readerDataValues[i] = DBNull.Value;
                                        break;
                                }
                            }
                        }
                        if (null != xml)
                        {
                            switch (this._xmlMap[i])
                            {
                                case SqlXml: // turn string into a SqlXml value for DataColumn
                                    System.Xml.XmlReaderSettings settings = new System.Xml.XmlReaderSettings();
                                    settings.ConformanceLevel = System.Xml.ConformanceLevel.Fragment;
                                    System.Xml.XmlReader reader = System.Xml.XmlReader.Create(new System.IO.StringReader(xml), settings, (string)null);
                                    this._readerDataValues[i] = new System.Data.SqlTypes.SqlXml(reader);
                                    break;
                                case XmlDocument: // turn string into XmlDocument value for DataColumn
                                    System.Xml.XmlDocument document = new System.Xml.XmlDocument();
                                    document.LoadXml(xml);
                                    this._readerDataValues[i] = document;
                                    break;
                            }
                            // default: let value fallthrough to DataSet which may fail with ArgumentException
                        }
                    }
                }
            }

            switch (this._mappedMode)
            {
                default:
                case MapExactMatch:
                    Debug.Assert(0 == this._mappedMode, "incorrect mappedMode");
                    Debug.Assert((null == this._chapterMap) && (null == this._indexMap) && (null == this._mappedDataValues), "incorrect MappedValues");
                    return this._readerDataValues;  // from reader to dataset
                case MapDifferentSize:
                    Debug.Assert((null == this._chapterMap) && (null == this._indexMap) && (null != this._mappedDataValues), "incorrect MappedValues");
                    this.MappedValues();
                    break;
                case MapReorderedValues:
                    Debug.Assert((null == this._chapterMap) && (null != this._indexMap) && (null != this._mappedDataValues), "incorrect MappedValues");
                    this.MappedIndex();
                    break;
                case MapChapters:
                    Debug.Assert((null != this._chapterMap) && (null == this._indexMap) && (null != this._mappedDataValues), "incorrect MappedValues");
                    this.MappedChapter();
                    break;
                case MapChaptersReordered:
                    Debug.Assert((null != this._chapterMap) && (null != this._indexMap) && (null != this._mappedDataValues), "incorrect MappedValues");
                    this.MappedChapterIndex();
                    break;
            }
            return this._mappedDataValues;
        }

        internal async Task LoadDataRowWithClearAsync( CancellationToken cancellationToken )
        {
            // for FillErrorEvent to ensure no values leftover from previous row
            for (int i = 0; i < this._readerDataValues.Length; ++i)
            {
                this._readerDataValues[i] = null;
            }

            await this.LoadDataRowAsync( cancellationToken ).ConfigureAwait(false);
        }

        internal async Task LoadDataRowAsync( CancellationToken cancellationToken )
        {
            try
            {
                _ = this._dataReader.GetValues(this._readerDataValues );
                object[] mapped = this.GetMappedValues();

                DataRow dataRow;
                switch (this._loadOption)
                {
                    case LoadOption.OverwriteChanges:
                    case LoadOption.PreserveChanges:
                    case LoadOption.Upsert:
                        dataRow = this._dataTable.LoadDataRow(mapped, this._loadOption);
                        break;
                    case (LoadOption)4: // true
                        dataRow = this._dataTable.LoadDataRow(mapped, true);
                        break;
                    case (LoadOption)5: // false
                        dataRow = this._dataTable.LoadDataRow(mapped, false);
                        break;
                    default:
                        Debug.Assert(false, "unexpected LoadOption");
                        throw ADP.InvalidLoadOption(this._loadOption);
                }

                if ((null != this._chapterMap) && (null != this._dataSet))
                {
                    await this.LoadDataRowChaptersAsync( dataRow, cancellationToken ).ConfigureAwait(false); // MDAC 70772
                }
            }
            finally
            {
                if (null != this._chapterMap)
                {
                    this.FreeDataRowChapters(); // MDAC 71900
                }
            }
        }

        private void FreeDataRowChapters()
        {
            for (int i = 0; i < this._chapterMap.Length; ++i)
            {
                if (this._chapterMap[i])
                {
                    IDisposable disposable = (this._readerDataValues[i] as IDisposable);
                    if (null != disposable)
                    {
                        this._readerDataValues[i] = null;
                        disposable.Dispose();
                    }
                }
            }
        }

        internal async Task<int> LoadDataRowChaptersAsync( DataRow dataRow, CancellationToken cancellationToken )
        {
            int datarowadded = 0;

            int rowLength = this._chapterMap.Length;
            for (int i = 0; i < rowLength; ++i)
            {
                if (this._chapterMap[i])
                {
                    object readerValue = this._readerDataValues[i];
                    if ((null != readerValue) && !Convert.IsDBNull(readerValue))
                    {
                        this._readerDataValues[i] = null;

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
                    Debug.Assert(null != this._dataSet, "if chapters, then Fill(DataSet,...) not Fill(DataTable,...)");

                    object parentChapterValue;
                    DataColumn parentChapterColumn;
                    if (null == this._indexMap)
                    {
                        parentChapterColumn = this._dataTable.Columns[i];
                        parentChapterValue = dataRow[parentChapterColumn];
                    }
                    else
                    {
                        parentChapterColumn = this._dataTable.Columns[this._indexMap[i]];
                        parentChapterValue = dataRow[parentChapterColumn];
                    }

                    // correct on Fill, not FillFromReader
                    string chapterTableName = this._tableMapping.SourceTable + this._fieldNames[i]; // MDAC 70908

                    AdaDataReaderContainer readerHandler = AdaDataReaderContainer.Create( dbDataReader, this._dataReader.ReturnProviderSpecificTypes );

                    var fillFromReaderResult = await this._adapter.FillFromReaderAsync( this._dataSet, null, chapterTableName, readerHandler, 0, 0, parentChapterColumn, parentChapterValue, cancellationToken ).ConfigureAwait(false);
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
            int count = this._dataReader.FieldCount;

            object[] dataValues = null;
            List<object> addedItems = null;
            try
            {
                DataColumnCollection columnCollection = this._dataTable.Columns;
                columnCollection.EnsureAdditionalCapacity_(count + (chapterValue != null ? 1 : 0));
                // We can always just create column if there are no existing column or column mappings, and the mapping action is passthrough
                bool alwaysCreateColumns = ((this._dataTable.Columns.Count == 0) && ((this._tableMapping.ColumnMappings == null) || (this._tableMapping.ColumnMappings.Count == 0)) && (mappingAction == MissingMappingAction.Passthrough));

                for (int i = 0; i < count; ++i)
                {

                    bool ischapter = false;
                    Type fieldType = this._dataReader.GetFieldType(i);

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
                        if (null == this._xmlMap)
                        { // map to DataColumn with DataType=typeof(SqlXml)
                            this._xmlMap = new int[count];
                        }
                        this._xmlMap[i] = SqlXml; // track its xml data
                    }
                    else if (typeof(System.Xml.XmlReader).IsAssignableFrom(fieldType))
                    {
                        fieldType = typeof(String); // map to DataColumn with DataType=typeof(string)
                        if (null == this._xmlMap)
                        {
                            this._xmlMap = new int[count];
                        }
                        this._xmlMap[i] = XmlDocument; // track its xml data
                    }

                    DataColumn dataColumn;
                    if (alwaysCreateColumns)
                    {
                        dataColumn = DataColumnReflection.CreateDataColumnBySchemaAction_(this._fieldNames[i], this._fieldNames[i], this._dataTable, fieldType, schemaAction);
                    }
                    else
                    {
                        dataColumn = this._tableMapping.GetDataColumn(this._fieldNames[i], fieldType, this._dataTable, mappingAction, schemaAction);
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
                    else if ((null != this._xmlMap) && (0 != this._xmlMap[i]))
                    {
                        if (typeof(System.Data.SqlTypes.SqlXml) == dataColumn.DataType)
                        {
                            this._xmlMap[i] = SqlXml;
                        }
                        else if (typeof(System.Xml.XmlDocument) == dataColumn.DataType)
                        {
                            this._xmlMap[i] = XmlDocument;
                        }
                        else
                        {
                            this._xmlMap[i] = 0; // datacolumn is not a specific Xml dataType, i.e. string

                            int total = 0;
                            for (int x = 0; x < this._xmlMap.Length; ++x)
                            {
                                total += this._xmlMap[x];
                            }
                            if (0 == total)
                            { // not mapping to a specific Xml datatype, get rid of the map
                                this._xmlMap = null;
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

                    chapterColumn = this._tableMapping.GetDataColumn(this._tableMapping.SourceTable, fieldType, this._dataTable, mappingAction, schemaAction);
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
                    if ((null != this._dataSet) && (null == this._dataTable.DataSet))
                    {
                        // Allowed to throw exception if DataTable is from wrong DataSet
                        AddItemToAllowRollback(ref addedItems, this._dataTable);
                        this._dataSet.Tables.Add(this._dataTable);
                    }
                    if (gettingData)
                    {
                        if (null == columnCollection)
                        {
                            columnCollection = this._dataTable.Columns;
                        }
                        this._indexMap = columnIndexMap;
                        this._chapterMap = chapterIndexMap;
                        dataValues = this.SetupMapping(count, columnCollection, chapterColumn, chapterValue);
                    }
                    else
                    {
                        // debug only, but for retail debug ability
                        this._mappedMode = -1;
                    }
                }
                else
                {
                    this._dataTable = null;
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
            AdaDbSchemaRow[] schemaRows = AdaDbSchemaRow.GetSortedSchemaRows(this._schemaTable, this._dataReader.ReturnProviderSpecificTypes); // MDAC 60609
            Debug.Assert(null != schemaRows, "SchemaSetup - null DbSchemaRow[]");
            Debug.Assert(this._dataReader.FieldCount <= schemaRows.Length, "unexpected fewer rows in Schema than FieldCount");

            if (0 == schemaRows.Length)
            {
                this._dataTable = null;
                return (object[])null;
            }

            // Everett behavior, always add a primary key if a primary key didn't exist before
            // Whidbey behavior, same as Everett unless using LoadOption then add primary key only if no columns previously existed
            bool addPrimaryKeys = (((0 == this._dataTable.PrimaryKey.Length) && ((4 <= (int)this._loadOption) || (0 == this._dataTable.Rows.Count)))
                                    || (0 == this._dataTable.Columns.Count)); // MDAC 67033

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
            DataColumnCollection columnCollection = this._dataTable.Columns;
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
                        fieldType = this._dataReader.GetFieldType(sortedIndex);
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
                        if (null == this._xmlMap)
                        {
                            this._xmlMap = new int[schemaRows.Length];
                        }
                        this._xmlMap[sortedIndex] = SqlXml;
                    }
                    else if (typeof(System.Xml.XmlReader).IsAssignableFrom(fieldType))
                    {
                        fieldType = typeof(String);
                        if (null == this._xmlMap)
                        {
                            this._xmlMap = new int[schemaRows.Length];
                        }
                        this._xmlMap[sortedIndex] = XmlDocument;
                    }

                    DataColumn dataColumn = null;
                    if (!schemaRow.IsHidden)
                    {
                        dataColumn = this._tableMapping.GetDataColumn(this._fieldNames[sortedIndex], fieldType, this._dataTable, mappingAction, schemaAction);
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
                    else if ((null != this._xmlMap) && (0 != this._xmlMap[sortedIndex]))
                    {
                        if (typeof(System.Data.SqlTypes.SqlXml) == dataColumn.DataType)
                        {
                            this._xmlMap[sortedIndex] = SqlXml;
                        }
                        else if (typeof(System.Xml.XmlDocument) == dataColumn.DataType)
                        {
                            this._xmlMap[sortedIndex] = XmlDocument;
                        }
                        else
                        {
                            this._xmlMap[sortedIndex] = 0; // datacolumn is not a specific Xml dataType, i.e. string

                            int total = 0;
                            for (int x = 0; x < this._xmlMap.Length; ++x)
                            {
                                total += this._xmlMap[x];
                            }
                            if (0 == total)
                            { // not mapping to a specific Xml datatype, get rid of the map
                                this._xmlMap = null;
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
                        if (4 <= (int)this._loadOption)
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
                        if (4 > (int)this._loadOption)
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
                    chapterColumn = this._tableMapping.GetDataColumn(this._tableMapping.SourceTable, fieldType, this._dataTable, mappingAction, schemaAction);
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
                    if ((null != this._dataSet) && null == this._dataTable.DataSet)
                    {
                        AddItemToAllowRollback(ref addedItems, this._dataTable);
                        this._dataSet.Tables.Add(this._dataTable);
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
                            this._dataTable.PrimaryKey = keys;
                        }
                        else
                        {
                            UniqueConstraint unique = new UniqueConstraint("", keys);
                            ConstraintCollection constraints = this._dataTable.Constraints;
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
                    if (!commonFromMultiTable && !string.IsNullOrEmpty(commonBaseTable) && string.IsNullOrEmpty(this._dataTable.TableName))
                    {
                        this._dataTable.TableName = commonBaseTable;
                    }
                    if (gettingData)
                    {
                        this._indexMap = columnIndexMap;
                        this._chapterMap = chapterIndexMap;
                        dataValues = this.SetupMapping(schemaRows.Length, columnCollection, chapterColumn, chapterValue);
                    }
                    else
                    {
                        // debug only, but for retail debug ability
                        this._mappedMode = -1;
                    }
                }
                else
                {
                    this._dataTable = null;
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
            if (null != this._dataSet)
            {
                string name = /*parentChapterColumn.ColumnName + "_" +*/ chapterColumn.ColumnName; // MDAC 72815

                DataRelation relation = new DataRelation(name, new DataColumn[] { parentChapterColumn }, new DataColumn[] { chapterColumn }, false); // MDAC 71878

                int index = 1;
                string tmp = name;
                DataRelationCollection relations = this._dataSet.Relations;
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

            if (null == this._indexMap)
            {
                int mappingCount = columnCollection.Count;
                bool hasChapters = (null != this._chapterMap);
                if ((count != mappingCount) || hasChapters)
                {
                    this._mappedDataValues = new object[mappingCount];
                    if (hasChapters)
                    {

                        this._mappedMode = MapChapters;
                        this._mappedLength = count;
                    }
                    else
                    {
                        this._mappedMode = MapDifferentSize;
                        this._mappedLength = Math.Min(count, mappingCount);
                    }
                }
                else
                {
                    this._mappedMode = MapExactMatch; /* _mappedLength doesn't matter */
                }
            }
            else
            {
                this._mappedDataValues = new object[columnCollection.Count];
                this._mappedMode = ((null == this._chapterMap) ? MapReorderedValues : MapChaptersReordered);
                this._mappedLength = count;
            }
            if (null != chapterColumn)
            { // value from parent tracked into child table
                this._mappedDataValues[chapterColumn.Ordinal] = chapterValue;
            }
            return dataValues;
        }
    }
}
