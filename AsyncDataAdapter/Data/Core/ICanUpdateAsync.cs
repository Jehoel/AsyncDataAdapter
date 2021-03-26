using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    public interface ICanUpdateAsync : IBatchingAdapter
    {
        DbCommand SelectCommand { get; }
        DbCommand InsertCommand { get; }
        DbCommand DeleteCommand { get; }
        DbCommand UpdateCommand { get; }

        DbConnection GetConnection();

        RowUpdatingEventArgs CreateRowUpdatingEvent( DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping );
        RowUpdatedEventArgs  CreateRowUpdatedEvent ( DataRow dataRow, DbCommand command, StatementType statementType, DataTableMapping tableMapping );

        void OnRowUpdating( RowUpdatingEventArgs e );
        void OnRowUpdated ( RowUpdatedEventArgs  e );

        void UpdatingRowStatusErrors( RowUpdatingEventArgs e, DataRow row );

        Int32 UpdatedRowStatus( RowUpdatedEventArgs e, BatchCommandInfo[] batchCommands, Int32 commandCount );

        Task UpdateRowExecuteAsync( RowUpdatedEventArgs rowUpdatedEvent, DbCommand dataCommand, StatementType cmdIndex, CancellationToken cancellationToken );

        Task<ConnectionState> UpdateConnectionOpenAsync( DbConnection connection, StatementType statementType, DbConnection[] connections, ConnectionState[] connectionStates, bool useSelectConnectionState, CancellationToken cancellationToken );
    }
}
