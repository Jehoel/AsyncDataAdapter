using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Reflection;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDbDataAdapter<TDbDataAdapter,TDbConnection,TDbCommand,TDbDataReader>
    {
        #region FillError

        private static readonly Object _fillErrorEventKeyFieldInfo = GetFillErrorEventKey();

        private static Object GetFillErrorEventKey()
        {
            if( Reflection.TryGetStaticFieldInfo( typeof(DataAdapter), name: "EventFillError", out FieldInfo dotNetFramework4x ) )
            {
                return dotNetFramework4x.GetValue( obj: null );
            }
            else if( Reflection.TryGetStaticFieldInfo( typeof(DataAdapter), name: "s_eventFillError", out FieldInfo dotNetCore31 ) )
            {
                return dotNetCore31.GetValue( obj: null );
            }
            else
            {
                throw new InvalidOperationException( "Couldn't find DataAdapter's static event-key field for FillError." );
            }
        }

        [DefaultValue(false)]
        public bool HasFillErrorHandler
        {
            get
            {
                Delegate d = base.Events[ _fillErrorEventKeyFieldInfo ];
                return d != null;
                // ah, don't need to check `GetInvocationList().Length > 0`... right?
            }
        }

        private void OnFillErrorHandler( Exception ex, DataTable dataTable = null, object[] dataValues = null )
        {
            FillErrorEventArgs fillErrorEvent = new FillErrorEventArgs( dataTable, dataValues )
            {
                Errors = ex
            };

            this.OnFillError( fillErrorEvent );

            if (!fillErrorEvent.Continue)
            {
                if (fillErrorEvent.Errors != null)
                {
                    throw fillErrorEvent.Errors;
                }

                throw ex;
            }
        }

        private Action<Exception, DataTable, Object[]> GetCurrentFillErrorHandler()
        {
            if( this.HasFillErrorHandler )
            {
                return this.OnFillErrorHandler;
            }

            return null;
        }

        #endregion
    }
}
