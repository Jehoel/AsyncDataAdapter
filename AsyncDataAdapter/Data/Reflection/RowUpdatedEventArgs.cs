using System;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace AsyncDataAdapter.Internal
{
    public static class RowUpdatedEventArgsReflection
    {
        private struct _AdapterInit { }

        /// <summary>Exposes <c>void <see cref="DataColumn"/>.AdapterInit(<see cref="Int32"/> recordsAffected)</c>.</summary>
        public static void AdapterInit_(this RowUpdatedEventArgs e, int recordsAffected)
        {
            ReflectedAction<RowUpdatedEventArgs,_AdapterInit,Int32>.Invoke( instance: e, recordsAffected );
        }

        /// <summary>Exposes <c>void <see cref="DataColumn"/>.AdapterInit(<see cref="DataRow"/>[] dataRows)</c>.</summary>
        public static void AdapterInit_(this RowUpdatedEventArgs e, DataRow[] dataRows)
        {
            ReflectedAction<RowUpdatedEventArgs,_AdapterInit,DataRow[]>.Invoke( instance: e, dataRows );
        }

        private struct _Rows { }

        /// <summary>Exposes <c><see cref="DataRow"/>[] <see cref="RowUpdatedEventArgs"/>.Rows</c> instance property.</summary>
        public static DataRow[] GetRows_(this RowUpdatedEventArgs e)
        {
            return ReflectedProperty<RowUpdatedEventArgs,_Rows,DataRow[]>.GetValue( e );
        }

        /// <summary>Convenience shorthand method to index-into elements of the <see cref="RowUpdatedEventArgs"/> private DataRow array field.</summary>
        public static DataRow GetRow_(this RowUpdatedEventArgs e, int row)
        {
            DataRow[] rows = GetRows_( e );
            if (rows is null)
            {
                throw new InvalidOperationException("The " + nameof(RowUpdatedEventArgs) + " instance has not yet been initialized.");
            }

            return rows[row];
        }
    }
}
