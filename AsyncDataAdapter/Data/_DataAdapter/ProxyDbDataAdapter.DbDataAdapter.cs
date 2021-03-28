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

        // `DbDataAdapter.FooCommand` wraps its `IDbDataAdapter.FooCommand`'s vtable.
        // ...which is weird.
        /*

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

        */

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

        private struct _Fill { }

        protected override Int32 Fill( DataSet dataSet, int startRecord, int maxRecords, string srcTable, IDbCommand command, CommandBehavior behavior )
        {
            return ReflectedFunc<DbDataAdapter,_Fill,DataSet,Int32,Int32,String,IDbCommand,CommandBehavior,Int32>.Invoke( this.Subject, dataSet, startRecord, maxRecords, srcTable, command, behavior );
        }

        private struct _FillSchema { }

        protected override DataTable[] FillSchema( DataSet dataSet, SchemaType schemaType, IDbCommand command, String srcTable, CommandBehavior behavior )
        {
            return ReflectedFunc<DbDataAdapter,_FillSchema,DataSet,SchemaType,IDbCommand,String,CommandBehavior,DataTable[]>.Invoke( this.Subject, dataSet, schemaType, command, srcTable, behavior );
        }

        protected override DataTable FillSchema( DataTable dataTable, SchemaType schemaType, IDbCommand command, CommandBehavior behavior )
        {
            return ReflectedFunc<DbDataAdapter,_FillSchema,DataTable,SchemaType,IDbCommand,CommandBehavior,DataTable>.Invoke( this.Subject, dataTable, schemaType, command, behavior );
        }

        private struct _Update { }

        protected override Int32 Update( DataRow[] dataRows, DataTableMapping tableMapping )
        {
            return ReflectedFunc<DbDataAdapter,_Update,DataRow[],DataTableMapping,Int32>.Invoke( this.Subject, dataRows, tableMapping );
        }

        #endregion
    }
}
