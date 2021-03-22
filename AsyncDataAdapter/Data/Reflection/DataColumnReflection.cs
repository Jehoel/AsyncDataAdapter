using System;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace AsyncDataAdapter.Internal
{
    public static class DataColumnReflection
    {
        private static readonly MethodInfo _IsAutoIncrementType_Type       = Reflection.GetStaticMethod<DataColumn>("IsAutoIncrementType", typeof(Type));
        private static readonly MethodInfo _EnsureAdditionalCapacity_Int32 = Reflection.GetInstanceMethod<DataColumnCollection>("EnsureAdditionalCapacity", typeof(int));
        private static readonly MethodInfo _CreateDataColumnBySchemaAction = Reflection.GetStaticMethod<DataColumnMapping>("CreateDataColumnBySchemaAction", typeof(string), typeof(string), typeof(DataTable), typeof(Type), typeof(MissingSchemaAction));

        /// <summary>Exposes <c>static void <see cref="DataColumn"/>.IsAutoIncrementType(Type dataType)</c>.</summary>
        public static bool IsAutoIncrementType_(Type dataType)
        {
             if (dataType is null) throw new ArgumentNullException(nameof(dataType));

            return _IsAutoIncrementType_Type.InvokeDisallowNull<bool>(@this: null, dataType);
        }

        /// <summary>Exposes <c>void <see cref="DataColumnCollection"/>.EnsureAdditionalCapacity(int capacity)</c>.</summary>
        public static void EnsureAdditionalCapacity_(this DataColumnCollection c, int capacity)
        {
             if (c is null) throw new ArgumentNullException(nameof(c));

            _EnsureAdditionalCapacity_Int32.InvokeVoid(@this: c, capacity);
        }

        /// <summary>Exposes <c>static <see cref="DataColumn"/> <see cref="DataColumnMapping"/>.CreateDataColumnBySchemaAction(string sourceColumn, string dataSetColumn, DataTable dataTable, Type dataType, MissingSchemaAction schemaAction)</c>.</summary>
        public static DataColumn CreateDataColumnBySchemaAction_(string sourceColumn, string dataSetColumn, DataTable dataTable, Type dataType, MissingSchemaAction schemaAction)
        {
            return _CreateDataColumnBySchemaAction.InvokeAllowNull<DataColumn>(@this: null, sourceColumn, dataSetColumn, dataTable, dataType, schemaAction);
        } 
    }
}
