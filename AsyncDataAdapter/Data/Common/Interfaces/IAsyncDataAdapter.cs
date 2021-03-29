using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    /// <summary>Extends <see cref="IDataAdapter"/> with support for async methods for read-only data adapter operations.</summary>
    public interface IAsyncDataAdapter : IDataAdapter
    {
        Task<Int32> FillAsync( DataSet dataSet, CancellationToken cancellationToken = default );

        Task<DataTable[]> FillSchemaAsync( DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken = default );
    }

    public interface IAsyncDbDataAdapter : IAsyncDataAdapter, IDbDataAdapter
    {
    }

    /// <summary>Extends <see cref="IAsyncDataAdapter"/> with support for <see cref="UpdateAsync(DataSet, CancellationToken)"/>.</summary>
    public interface IUpdatingAsyncDataAdapter : IAsyncDataAdapter
    {
        Task<Int32> UpdateAsync( DataSet dataSet, CancellationToken cancellationToken = default );
    }

    /// <summary>Intersection-type of <see cref="IUpdatingAsyncDataAdapter"/> and <see cref="IDbDataAdapter"/> (because <see cref="IAsyncDataAdapter"/> does not extend <see cref="IDbDataAdapter"/>).</summary>
    public interface IUpdatingAsyncDbDataAdapter : IUpdatingAsyncDataAdapter, IDbDataAdapter
    {
    }

}
