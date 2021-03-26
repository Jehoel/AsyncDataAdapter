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

    public interface IUpdatingAsyncDbDataAdapter : IUpdatingAsyncDataAdapter, IDbDataAdapter
    {
    }

    public static class AsyncDataAdapterExtensions
    {
        // `DataAdapter` only defines 1 public overload of Fill, FillSchema, and Update - so we don't need to supply overloads for IAsyncDataAdapter, fwiw.
        // But `DbDataAdapter` does provide a bunch of non-virtual public methods...

        //  public    override Int32       Fill( DataSet dataSet )
        //  public             Int32       Fill( DataSet dataSet, string srcTable )
        //  public             Int32       Fill( DataSet dataSet, int startRecord, int maxRecords, string srcTable )
        //  public             Int32       Fill( DataTable dataTable )
        //  public             Int32       Fill( int startRecord, int maxRecords, params DataTable[] dataTables )
        //  protected virtual  Int32       Fill( DataSet dataSet, int startRecord, int maxRecords, string srcTable, IDbCommand command, CommandBehavior behavior )
        //
        //  public             DataTable   FillSchema( DataTable dataTable, SchemaType schemaType )
        //  public    override DataTable[] FillSchema( DataSet   dataSet  , SchemaType schemaType )
        //  public             DataTable[] FillSchema( DataSet   dataSet  , SchemaType schemaType, string srcTable )
        //  protected virtual  DataTable[] FillSchema( DataSet   dataSet  , SchemaType schemaType, IDbCommand command, string srcTable, CommandBehavior behavior )
        //  protected virtual  DataTable   FillSchema( DataTable dataTable, SchemaType schemaType, IDbCommand command                 , CommandBehavior behavior )
        //
        //  public    override Int32       Update( DataSet   dataSet   )
        //  public             Int32       Update( DataRow[] dataRows  )
        //  public             Int32       Update( DataTable dataTable )
        //  public             Int32       Update( DataSet   dataSet , string srcTable )
        //  protected virtual  Int32       Update( DataRow[] dataRows, DataTableMapping tableMapping )

        // But IDbDataAdapter (and IDataAdapter)'s methods are not the virtual methods - nor with many parameters:
        //  
        //  Int32       Fill      ( DataSet dataSet );
        //  DataTable[] FillSchema( DataSet dataSet, SchemaType schemaType );
        //  Int32       Update    ( DataSet dataSet );

        // BTW, the following methods on DbDataAdapter are virtual (there are no abstract membesr) and will always throw (so they SHOULD have been marked as abstract):
        /*

        protected virtual int            AddToBatch(IDbCommand command)
        protected virtual void           ClearBatch()
        protected virtual int            ExecuteBatch()
        protected virtual IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex)
        protected virtual void           InitializeBatching()
        protected virtual void           TerminateBatching()

        */

        public static Task<Int32> FillAsync( this IAsyncDataAdapter adapter, DataSet dataSet, string srcTable, CancellationToken cancellationToken = default )
        {
            if (adapter is null) throw new ArgumentNullException(nameof(adapter));

            //return adapter.FillAsync(  );
            throw new NotImplementedException();
        }

        public static async Task<Int32> FillAsync( this IAsyncDataAdapter adapter, DataSet dataSet, int startRecord, int maxRecords, string srcTable, CancellationToken cancellationToken = default )
        {
            throw new NotImplementedException();
        }

        public static async Task<Int32> FillAsync( this IAsyncDataAdapter adapter, DataTable dataTable, CancellationToken cancellationToken = default )
        {
            throw new NotImplementedException();
        }


        public static async Task<DataTable> FillSchemaAsync( this IAsyncDataAdapter adapter, DataTable dataTable, SchemaType schemaType, CancellationToken cancellationToken = default )
        {
            throw new NotImplementedException();
        }

        public static async Task<DataTable[]> FillSchemaAsync( this IAsyncDataAdapter adapter, DataSet dataSet, SchemaType schemaType, string srcTable, CancellationToken cancellationToken = default )
        {
            throw new NotImplementedException();
        }


        public static async Task<Int32> UpdateAsync( this IAsyncDataAdapter adapter, DataRow[] dataRows, CancellationToken cancellationToken = default )
        {
            throw new NotImplementedException();
        }

        public static async Task<Int32> UpdateAsync( this IAsyncDataAdapter adapter, DataTable dataTable, CancellationToken cancellationToken = default )
        {
            throw new NotImplementedException();
        }

        public static async Task<Int32> UpdateAsync( this IAsyncDataAdapter adapter, DataSet dataSet, string srcTable, CancellationToken cancellationToken = default )
        {
            throw new NotImplementedException();
        }
    }
}
