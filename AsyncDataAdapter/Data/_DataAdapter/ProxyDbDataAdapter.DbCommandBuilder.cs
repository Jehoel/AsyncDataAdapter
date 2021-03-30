using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader>
    {
        public virtual async Task<DbCommandBuilder<TDbCommand>> CreateCommandBuilderAsync( CancellationToken cancellationToken = default )
        {
            DbCommandBuilder normalBuilder = this.CreateCommandBuilder();

            DbCommandBuilder<TDbCommand> proxiedBuilder = await this.CreateProxiedCommandBuilderAsync( normalBuilder, cancellationToken ).ConfigureAwait(false);

            return proxiedBuilder;
        }

        protected abstract DbCommandBuilder CreateCommandBuilder();

        protected virtual async Task<DbCommandBuilder<TDbCommand>> CreateProxiedCommandBuilderAsync( DbCommandBuilder builder, CancellationToken cancellationToken = default )
        {
            DataTable selectCommandResultsSchema;
            using( DbDataReader reader = await this.SelectCommand.ExecuteReaderAsync( CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo, cancellationToken ).ConfigureAwait(false) )
            {
                selectCommandResultsSchema = reader.GetSchemaTable();
            }

            ProxyDbCommandBuilder<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> proxiedBuilder = new ProxyDbCommandBuilder<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader>(
                subject                   : builder,
                selectCommandResultsSchema: selectCommandResultsSchema
            );

            return proxiedBuilder;
        }
    }
}
