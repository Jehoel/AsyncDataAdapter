using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{
    /// <summary>Extends <see cref="IDataAdapter"/> with support for async methods.</summary>
    public interface IDataAdapter2 : IDataAdapter
    {
        Task<Int32> FillAsync( DataSet dataSet, CancellationToken cancellationToken = default );

        Task<DataTable[]> FillSchemaAsync( DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken = default );
    }

    /// <summary>Extends <see cref="IDataAdapter2"/> with <see cref="IDataAdapter3.UpdateAsync(DataSet, CancellationToken)"/>.</summary>
    public interface IDataAdapter3 : IDataAdapter2
    {
        Task<Int32> UpdateAsync( DataSet dataSet, CancellationToken cancellationToken = default );
    }
}
