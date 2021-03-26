using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader> : IDbDataAdapter
    {
        #region Reimplement IDbDataAdapter

        IDbCommand IDbDataAdapter.DeleteCommand
        {
            get => this.DeleteCommand;
            set => this.DeleteCommand = (TDbCommand)value;
        }

        IDbCommand IDbDataAdapter.InsertCommand
        {
            get => this.InsertCommand;
            set => this.InsertCommand = (TDbCommand)value;
        }

        IDbCommand IDbDataAdapter.SelectCommand
        {
            get => this.SelectCommand;
            set => this.SelectCommand = (TDbCommand)value;
        }

        IDbCommand IDbDataAdapter.UpdateCommand
        {
            get => this.UpdateCommand;
            set => this.UpdateCommand = (TDbCommand)value;
        }

        #endregion

        #region DbDataAdapter virtuals

        public override Int32 UpdateBatchSize
        {
            get => this.Subject.UpdateBatchSize;
            set
            {
                this.Subject.UpdateBatchSize = value;
                base.UpdateBatchSize = value;
            }
        }

        private static readonly MethodInfo _DbDataAdapter_Fill_6 = Reflection.GetInstanceMethod<TDbDataAdapter>( name: nameof(DbDataAdapter.Fill), paramTypes: new Type[] { typeof(DataSet), typeof(Int32), typeof(Int32), typeof(String), typeof(IDbCommand), typeof(CommandBehavior) } );

        protected override Int32 Fill( DataSet dataSet, int startRecord, int maxRecords, string srcTable, IDbCommand command, CommandBehavior behavior )
        {
            //return this.Subject.Fill( dataSet, startRecord: startRecord, maxRecords: maxRecords, srcTable, command, behavior );
            return _DbDataAdapter_Fill_6.InvokeDisallowNull<Int32>( this.Subject, dataSet, startRecord, maxRecords, srcTable, command, behavior );
        }

        private static readonly MethodInfo _DbDataAdapter_FillSchema_5 = Reflection.GetInstanceMethod<TDbDataAdapter>( name: nameof(DbDataAdapter.FillSchema), paramTypes: new Type[] { typeof(DataSet), typeof(SchemaType), typeof(IDbCommand), typeof(String), typeof(CommandBehavior) } );

        protected override DataTable[] FillSchema( DataSet dataSet, SchemaType schemaType, IDbCommand command, String srcTable, CommandBehavior behavior )
        {
            return _DbDataAdapter_FillSchema_5.InvokeAllowNull<DataTable[]>( this.Subject, dataSet, schemaType, command, srcTable, behavior );
        }

        private static readonly MethodInfo _DbDataAdapter_FillSchema_4 = Reflection.GetInstanceMethod<TDbDataAdapter>( name: nameof(DbDataAdapter.FillSchema), paramTypes: new Type[] { typeof(DataSet), typeof(SchemaType), typeof(IDbCommand), typeof(CommandBehavior) } );

        protected override DataTable FillSchema( DataTable dataTable, SchemaType schemaType, IDbCommand command, CommandBehavior behavior )
        {
            return _DbDataAdapter_FillSchema_4.InvokeAllowNull<DataTable>( this.Subject, dataTable, schemaType, command, behavior );
        }

        private static readonly MethodInfo _DbDataAdapter_Update = Reflection.GetInstanceMethod<TDbDataAdapter>( name: nameof(DbDataAdapter.Update), paramTypes: new Type[] { typeof(DataRow[]), typeof(DataTableMapping) } );

        protected override Int32 Update( DataRow[] dataRows, DataTableMapping tableMapping )
        {
            return _DbDataAdapter_Update.InvokeDisallowNull<Int32>( this.Subject, dataRows, tableMapping );
        }

        #endregion
    }
}
