using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Reflection;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter
{
    public abstract partial class ProxyDataAdapter : DataAdapter, IDataAdapter
    {
        #region FillError

        private static readonly Object _fillErrorEventKeyFieldInfo = GetFillErrorEventKey();

        private static Object GetFillErrorEventKey()
        {
            FieldInfo fieldInfo = Reflection.GetStaticFieldInfo( typeof(DataAdapter), name: "EventFillError" );
            return fieldInfo.GetValue( obj: null );
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

        #endregion
    }
}
