using System;
using System.Reflection;

using AsyncDataAdapter.SqlClient;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.Internal
{
    /// <summary>Exposes <c>Microsoft.Data.SqlClient.SqlCommandSet</c>. This class's type-constructor may fail if SqlCommandSet is not available on this platform, in which case consuming applications will need to reimplement <see cref="ISqlCommandSet"/>.</summary>
    public static class SqlCommandSetReflection
    {
        public static readonly Type _sqlCommandSetType = typeof(SqlCommand).Assembly.GetType(name: "Microsoft.Data.SqlClient.SqlCommandSet", throwOnError: true, ignoreCase: false);

        public static readonly MethodInfo _Connection_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.Connection), propertyType: typeof(SqlConnection));
        public static readonly MethodInfo _Connection_Set = Reflection.GetInstancePropertySetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.Connection), propertyType: typeof(SqlConnection));

        public static readonly MethodInfo _Transaction_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.Transaction), propertyType: typeof(SqlTransaction));
        public static readonly MethodInfo _Transaction_Set = Reflection.GetInstancePropertySetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.Transaction), propertyType: typeof(SqlTransaction));

        public static readonly MethodInfo _CommandTimeout_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.CommandTimeout), propertyType: typeof(Int32));
        public static readonly MethodInfo _CommandTimeout_Set = Reflection.GetInstancePropertySetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.CommandTimeout), propertyType: typeof(Int32));

        public static readonly MethodInfo _CommandCount_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.CommandCount), propertyType: typeof(Int32));

        public static readonly MethodInfo _Append_SqlCommand = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.Append), paramTypes: typeof(SqlCommand));

        public static readonly MethodInfo _Clear = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.Clear));
        public static readonly MethodInfo _Dispose = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.Dispose));

        public static readonly MethodInfo _ExecuteNonQuery = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.ExecuteNonQuery));

        public static readonly MethodInfo _GetParameterCount_Int32 = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.GetParameterCount), typeof(Int32));

        public static readonly MethodInfo _GetParameter_Int32_Int32 = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.GetParameter), typeof(Int32), typeof(Int32));

        public static readonly MethodInfo _GetBatchedAffected_Int32_Int32_Exception = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.GetBatchedAffected), typeof(Int32), typeof(Int32), typeof(Exception));

        //

        public static Object CreateInstance()
        {
            Object instance = Activator.CreateInstance(type: _sqlCommandSetType);
            return instance;
        }
    }
}
