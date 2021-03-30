using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    public static partial class AsyncDataReaderMethods
    {
        public static AdaSchemaMapping FillMapping( Action<Exception, DataTable, Object[]> onFillError, IAdaSchemaMappingAdapter adapter, DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue )
        {
            try
            {
                // only catch if a FillErrorEventHandler is registered so that in the default case we get the full callstack from users
                AdaSchemaMapping mapping = FillMappingInternal( adapter, dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue );
                return mapping;
            }
            catch (Exception ex) when(onFillError != null && ADP.IsCatchableExceptionType(ex))
            {
                onFillError( ex, null, null );
                return null;
            }
        }

        public static AdaSchemaMapping FillMappingInternal( IAdaSchemaMappingAdapter adapter, DataSet dataset, DataTable datatable, string srcTable, AdaDataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue )
        {
            bool withKeyInfo = (MissingSchemaAction.AddWithKey == adapter.MissingSchemaAction);
            
            string sourceTableName = null;
            if (dataset != null)
            {
                sourceTableName = GetSourceTableName( srcTable, schemaCount );
            }

            return new AdaSchemaMapping( adapter, dataset, datatable, dataReader, withKeyInfo, SchemaType.Mapped, sourceTableName, true, parentChapterColumn, parentChapterValue );
        }

        private static string GetSourceTableName(string srcTable, int index)
        {
            //if ((null != srcTable) && (0 <= index) && (index < srcTable.Length)) {
            if (0 == index)
            {
                return srcTable; //[index];
            }

            return srcTable + index.ToString(CultureInfo.InvariantCulture);
        }
    }
}
