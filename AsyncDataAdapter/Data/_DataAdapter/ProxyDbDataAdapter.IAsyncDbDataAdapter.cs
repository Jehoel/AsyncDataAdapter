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

        public Task<Int32> FillAsync(DataSet dataSet, CancellationToken cancellationToken = default )
        {
            TDbCommand      selectCommand       = this.SelectCommand;
		    CommandBehavior fillCommandBehavior = this.FillCommandBehavior;

		    return this.FillAsync( dataSet, 0, 0, "Table", selectCommand, fillCommandBehavior, cancellationToken );
        }

        public async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken = default)
        {
            TDbCommand selectCommand = this.SelectCommand;

		    if ( base.DesignMode && ( selectCommand == null || selectCommand.Connection == null || String.IsNullOrEmpty( selectCommand.CommandText ) ) )
		    {
			    return Array.Empty<DataTable>();
		    }

		    CommandBehavior fillCommandBehavior = FillCommandBehavior;

		    return await this.FillSchemaAsync( dataSet, schemaType, selectCommand, "Table", fillCommandBehavior, cancellationToken ).ConfigureAwait(false);
        }

        #endregion

        #region IUpdatingAsyncDataAdapter

        public Task<Int32> UpdateAsync(DataSet dataSet, CancellationToken cancellationToken = default )
        {
            if (!this.TableMappings.Contains(DbDataAdapter.DefaultSourceTableName))
            {
                string msg = string.Format("Update unable to find TableMapping['{0}'] or DataTable '{0}'.", DbDataAdapter.DefaultSourceTableName);
                throw new InvalidOperationException(msg);
            }

            return this.UpdateAsync( dataSet, srcTable: AdaDbDataAdapter.DefaultSourceTableName, cancellationToken );
        }

        #endregion
    }
}
