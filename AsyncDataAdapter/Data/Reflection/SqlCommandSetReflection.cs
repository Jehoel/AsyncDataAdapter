using System;
using System.Collections.Generic;
using System.Reflection;

using AsyncDataAdapter.SqlClient;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.Internal
{
    /// <summary>Exposes <c>Microsoft.Data.SqlClient.SqlCommandSet</c>. This class's type-constructor may fail if SqlCommandSet is not available on this platform, in which case consuming applications will need to reimplement <see cref="ISqlCommandSet"/>.</summary>
    public static class SqlCommandSetReflection
    {
        public static readonly Type _sqlCommandSetType = typeof(SqlCommand).Assembly.GetType(name: "Microsoft.Data.SqlClient.SqlCommandSet", throwOnError: true, ignoreCase: false);

        public static readonly Type _sqlCommandSetLocalCommandType = typeof(SqlCommand).Assembly.GetType(name: "Microsoft.Data.SqlClient.SqlCommandSet+LocalCommand", throwOnError: true, ignoreCase: false);

        public static readonly Type _localCommandListType = typeof(List<>).MakeGenericType( _sqlCommandSetLocalCommandType );

        //

        public static readonly MethodInfo _Connection_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.Connection), propertyType: typeof(SqlConnection));
        public static readonly MethodInfo _Connection_Set = Reflection.GetInstancePropertySetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.Connection), propertyType: typeof(SqlConnection));

        public static readonly MethodInfo _BatchCommand_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.BatchCommand), propertyType: typeof(SqlCommand));

        public static readonly MethodInfo _Transaction_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.Transaction), propertyType: typeof(SqlTransaction));
        public static readonly MethodInfo _Transaction_Set = Reflection.GetInstancePropertySetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.Transaction), propertyType: typeof(SqlTransaction));

        public static readonly MethodInfo _CommandTimeout_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.CommandTimeout), propertyType: typeof(Int32));
        public static readonly MethodInfo _CommandTimeout_Set = Reflection.GetInstancePropertySetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.CommandTimeout), propertyType: typeof(Int32));

        public static readonly MethodInfo _CommandList_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.CommandList), propertyType: _localCommandListType);

        public static readonly MethodInfo _CommandCount_Get = Reflection.GetInstancePropertyGetter(_sqlCommandSetType, name: nameof(ISqlCommandSet.CommandCount), propertyType: typeof(Int32));

        public static readonly MethodInfo _Append_SqlCommand = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.Append), paramTypes: typeof(SqlCommand));

        public static readonly MethodInfo _Clear = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.Clear));
        public static readonly MethodInfo _Dispose = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.Dispose));

        public static readonly MethodInfo _GetParameterCount_Int32 = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.GetParameterCount), typeof(Int32));

        public static readonly MethodInfo _GetParameter_Int32_Int32 = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.GetParameter), typeof(Int32), typeof(Int32));

        public static readonly MethodInfo _GetBatchedAffected_Int32_Int32_Exception = Reflection.GetInstanceMethod(_sqlCommandSetType, name: nameof(ISqlCommandSet.GetBatchedAffected), typeof(Int32), typeof(Int32), typeof(Exception));

        //

        public static Object CreateSqlCommandSet()
        {
            Object instance = Activator.CreateInstance(type: _sqlCommandSetType);
            return instance;
        }
    }

    public static class SqlCommandReflection
    {
        private static readonly Type _sqlRpcType = typeof(SqlCommand).Assembly.GetType(name: "Microsoft.Data.SqlClient._SqlRPC", throwOnError: true, ignoreCase: false);

        private static readonly Type _sqlRpcArrayType = _sqlRpcType.MakeArrayType();
        private static readonly Type _sqlRpcListType  = typeof(List<>).MakeGenericType( _sqlRpcType );

        public static Array CreateSqlRpcArray( Int32 length )
        {
            return Array.CreateInstance( elementType: _sqlRpcType, length: length );
        }

        public static readonly FieldInfo _CurrentlyExecutingBatch = Reflection.GetInstanceFieldInfo<SqlCommand>(name: "_currentlyExecutingBatch", fieldType: typeof(Int32));
        public static readonly FieldInfo _SqlRPCBatchArray        = Reflection.GetInstanceFieldInfo<SqlCommand>(name: "_SqlRPCBatchArray"       , fieldType: _sqlRpcArrayType);
        public static readonly FieldInfo _SqlRPCList              = Reflection.GetInstanceFieldInfo<SqlCommand>(name: "_RPCList"                , fieldType: _sqlRpcListType);

        public static readonly MethodInfo _BatchRPCMode_Get = Reflection.GetInstancePropertyGetter<SqlCommand>(name: "BatchRPCMode", propertyType: typeof(bool));
        public static readonly MethodInfo _BatchRPCMode_Set = Reflection.GetInstancePropertySetter<SqlCommand>(name: "BatchRPCMode", propertyType: typeof(bool));

        /// <summary>void</summary>
        public static readonly MethodInfo _ClearBatchCommand = Reflection.GetInstanceMethod<SqlCommand>(name: "ClearBatchCommand");

        /// <summary>void</summary>
        public static readonly MethodInfo _AddBatchCommand = Reflection.GetInstanceMethod<SqlCommand>(name: "AddBatchCommand", typeof(string), typeof(SqlParameterCollection), typeof(System.Data.CommandType), typeof(SqlCommandColumnEncryptionSetting));

        /// <summary>Returns a single <see cref="Int32"/></summary>
        public static readonly MethodInfo _ExecuteBatchRPCCommand = Reflection.GetInstanceMethod<SqlCommand>(name: "ExecuteBatchRPCCommand");
    }

    public static class LocalCommandReflection
    {
        private static readonly Type _type = SqlCommandSetReflection._sqlCommandSetLocalCommandType;

        public static readonly FieldInfo _CommandText             = Reflection.GetInstanceFieldInfo( type: _type, "CommandText"            , fieldType: typeof(string) );
        public static readonly FieldInfo _Parameters              = Reflection.GetInstanceFieldInfo( type: _type, "Parameters"             , fieldType: typeof(SqlParameterCollection) );
        public static readonly FieldInfo _CmdType                 = Reflection.GetInstanceFieldInfo( type: _type, "CmdType"                , fieldType: typeof(System.Data.CommandType) );
        public static readonly FieldInfo _ColumnEncryptionSetting = Reflection.GetInstanceFieldInfo( type: _type, "ColumnEncryptionSetting", fieldType: typeof(SqlCommandColumnEncryptionSetting) );
    }
}
