using System;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace AsyncDataAdapter.Internal
{
    public static class RowUpdatedEventArgsReflection
    {
        private static readonly MethodInfo _AdapterInit_Int32    = Reflection.GetInstanceMethod<RowUpdatedEventArgs>("AdapterInit", typeof(int));
        private static readonly MethodInfo _AdapterInit_DataRows = Reflection.GetInstanceMethod<RowUpdatedEventArgs>("AdapterInit", typeof(DataRow[]));
        private static readonly MethodInfo _GetRows              = Reflection.GetInstancePropertyGetter<RowUpdatedEventArgs>("Rows", propertyType: typeof(DataRow[]));

        /// <summary>Exposes <c>void <see cref="DataColumn"/>.IsAutoIncrementType(Type dataType)</c>.</summary>
        public static void AdapterInit_(this RowUpdatedEventArgs e, int rowCount)
        {
            if (e is null) throw new ArgumentNullException(nameof(e));

            _AdapterInit_Int32.InvokeVoid(@this: e, rowCount);
        }

        /// <summary>Exposes <c>void <see cref="DataColumn"/>.IsAutoIncrementType(Type dataType)</c>.</summary>
        public static void AdapterInit_(this RowUpdatedEventArgs e, DataRow[] rowBatch)
        {
            if (e is null) throw new ArgumentNullException(nameof(e));

            _AdapterInit_DataRows.InvokeVoid(@this: e, new object[] { rowBatch }); // Can't use implicit params[] because csc maps `DataRow[] rowBatch` directly to `object[] args`.
        }

        /// <summary>Exposes <c><see cref="DataRow"/>[] <see cref="RowUpdatedEventArgs"/>.Rows</c> instance property.</summary>
        public static DataRow[] GetRows_(this RowUpdatedEventArgs e)
        {
            if (e is null) throw new ArgumentNullException(nameof(e));

            return _GetRows.InvokeAllowNull<DataRow[]>(@this: e);
        }

        /// <summary>Convenience shorthand method to index-into elements of the <see cref="RowUpdatedEventArgs"/> private DataRow array.</summary>
        public static DataRow GetRow_(this RowUpdatedEventArgs e, int row)
        {
            if (e is null) throw new ArgumentNullException(nameof(e));

            DataRow[] rows = _GetRows.InvokeAllowNull<DataRow[]>(@this: e); // allow `null` return values so we can throw our own exception:
            if (rows is null)
            {
                throw new InvalidOperationException("The " + nameof(RowUpdatedEventArgs) + " instance has not yet been initialized.");
            }

            return rows[row];
        }
    }
}
