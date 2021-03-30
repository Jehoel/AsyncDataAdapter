using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    public interface IAdaSchemaMappingAdapter
    {
        LoadOption FillLoadOption { get; }

        Boolean AcceptChangesDuringFill { get; }

        MissingMappingAction MissingMappingAction { get; }
        MissingSchemaAction  MissingSchemaAction  { get; }

        DataTableMappingCollection TableMappings { get; }

        DataTableMapping GetTableMappingBySchemaAction( string sourceTableName, string dataSetTableName, MissingMappingAction mappingAction );

        int IndexOfDataSetTable(string dataSetTable);

        Task<int> FillFromReaderAsync( DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue, CancellationToken cancellationToken );
    }
}
