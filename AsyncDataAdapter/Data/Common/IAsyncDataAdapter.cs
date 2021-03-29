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

    /// <summary>Interface for all Fill, FillSchema, and Update method overloads. This interface type is not intended for use by consuming applications - but to aid my personal understanding of the confusing class design in <c>System.Data.Common</c>... and to simplify my extension methods used in tests.</summary>
    public interface IFullDbDataAdapter
    {
        #region Fill

        /// <summary>Same as the virtual <see cref="System.Data.Common.DataAdapter.Fill(DataSet)"/>. This is the only public <c>Fill</c> method on that type. It is also overridden by <see cref="System.Data.Common.DbDataAdapter.Fill(DataSet)"/>.</summary>
        Int32 Fill(DataSet dataSet);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.Fill(DataSet,string)"/>.</summary>
        Int32 Fill(DataSet dataSet, string srcTable);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.Fill(DataSet,int,int,string)"/>.</summary>
        Int32 Fill(DataSet dataSet, int startRecord, int maxRecords, string srcTable);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.Fill(DataTable)"/>.</summary>
        Int32 Fill(DataTable dataTable);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.Fill(int,int,DataTable[])"/>.</summary>
        Int32 Fill(int startRecord, int maxRecords, params DataTable[] dataTables);

        #endregion

        #region FillSchema

        /// <summary>Same as the virtual <see cref="System.Data.Common.DataAdapter.FillSchema(DataSet, SchemaType)"/>. It is also overridden by <see cref="System.Data.Common.DbDataAdapter.FillSchema(DataSet, SchemaType)"/>.</summary>
        DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.FillSchema(DataTable, SchemaType)"/>.</summary>
        DataTable FillSchema(DataTable dataTable, SchemaType schemaType);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.FillSchema(DataSet, SchemaType, string)"/>.</summary>
        DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType, string srcTable);

        #endregion

        #region Update

        /// <summary>Same as the virtual <see cref="System.Data.Common.DataAdapter.Update(DataSet)"/>. It is also overridden by <see cref="System.Data.Common.DbDataAdapter.Update(DataSet)"/>.</summary>
        Int32 Update(DataSet dataSet);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.Update(DataRow[])"/>.</summary>
        Int32 Update(DataRow[] dataRows);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.Update(DataTable)"/>.</summary>
        Int32 Update(DataTable dataTable);

        /// <summary>Same as the non-virtual <see cref="System.Data.Common.DbDataAdapter.Update(DataSet, string)"/>.</summary>
        Int32 Update(DataSet dataSet, string srcTable);

        #endregion
    }

    /// <summary>Interface for all Fill, FillSchema, and Update method overloads. This interface type is not intended for use by consuming applications - but to aid my personal understanding of the confusing class design in <c>System.Data.Common</c>... and to simplify my extension methods used in tests.</summary>
    public interface IFullAsyncDbDataAdapter
    {
        #region Fill

        Task<Int32> FillAsync(DataSet dataSet, CancellationToken cancellationToken);

        Task<Int32> FillAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken);

        Task<Int32> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, CancellationToken cancellationToken);

        Task<Int32> FillAsync(DataTable dataTable, CancellationToken cancellationToken);

        Task<Int32> FillAsync(int startRecord, int maxRecords, DataTable[] dataTables, CancellationToken cancellationToken);

        #endregion

        #region FillSchema

        Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken);

        Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, CancellationToken cancellationToken);

        Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, CancellationToken cancellationToken);

        #endregion

        #region Update

        Task<Int32> UpdateAsync(DataSet dataSet, CancellationToken cancellationToken);

        Task<Int32> UpdateAsync(DataRow[] dataRows, CancellationToken cancellationToken);

        Task<Int32> UpdateAsync(DataTable dataTable, CancellationToken cancellationToken);

        Task<Int32> UpdateAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken);

        #endregion
    }

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
}
