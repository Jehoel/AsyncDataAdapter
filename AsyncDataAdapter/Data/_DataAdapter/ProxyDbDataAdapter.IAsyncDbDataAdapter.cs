using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> : IUpdatingAsyncDbDataAdapter
    {
        #region IAsyncDataReader

        public async Task<Int32> FillAsync(DataSet dataSet, CancellationToken cancellationToken = default )
        {

        }

        public async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken = default)
        {

        }

        #endregion

        #region IUpdatingAsyncDataAdapter

        public async Task<Int32> UpdateAsync(DataSet dataSet, CancellationToken cancellationToken = default )
        {

        }

        #endregion
    }
}
