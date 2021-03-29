using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader>
    {
        public virtual async Task<DbCommandBuilder> CreateCommandBuilderAsync( CancellationToken cancellationToken = default )
        {
            DbCommandBuilder normalBuilder = this.CreateCommandBuilder();

            DbCommandBuilder proxiedBuilder = await this.CreateProxiedCommandBuilderAsync( normalBuilder ).ConfigureAwait(false);

            return proxiedBuilder;
        }

        protected abstract DbCommandBuilder CreateCommandBuilder();

        protected virtual async Task<DbCommandBuilder> CreateProxiedCommandBuilderAsync( DbCommandBuilder builder, CancellationToken cancellationToken = default )
        {
            DataTable selectCommandResultsSchema;
            using( DbDataReader reader = await this.SelectCommand.ExecuteReaderAsync( CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo ).ConfigureAwait(false) )
            {
                selectCommandResultsSchema = reader.GetSchemaTable();
            }

            ProxyDbCommandBuilder<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> proxiedBuilder = new ProxyDbCommandBuilder<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader>(
                subject                   : builder,
                proxyDataAdapter          : this,
                selectCommandResultsSchema: selectCommandResultsSchema
            );

            return proxiedBuilder;
        }
    }
}
