using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.SqlClient
{
    /// <summary>Implements <see cref="ISqlCommandSet"/> by using reflection to use the original and underlying <c>Microsoft.Data.SqlClient.SqlCommandSet</c>.</summary>
    public class SqlCommandSetWrapper : ISqlCommandSet
    {
        private readonly Object instance;

        internal SqlCommandSetWrapper(Object instance)
        {
//          this.instance = SqlCommandSetReflection.CreateInstance();
            this.instance = instance ?? throw new ArgumentNullException(nameof(instance));
        }

        public SqlConnection Connection
        {
            get => SqlCommandSetReflection._Connection_Get.InvokeAllowNull<SqlConnection>(this.instance);
            set => SqlCommandSetReflection._Connection_Set.InvokeVoid(this.instance, value);
        }

        public SqlTransaction Transaction
        {
            get => SqlCommandSetReflection._Transaction_Get.InvokeAllowNull<SqlTransaction>(this.instance);
            set => SqlCommandSetReflection._Transaction_Set.InvokeVoid(this.instance, value);
        }
        public int CommandTimeout
        {
            get => SqlCommandSetReflection._CommandTimeout_Get.InvokeDisallowNull<int>(this.instance);
            set => SqlCommandSetReflection._CommandTimeout_Set.InvokeVoid(this.instance, value);
        }

        public int CommandCount
        {
            get => SqlCommandSetReflection._CommandCount_Get.InvokeDisallowNull<int>(this.instance);
        }

        public void Append(SqlCommand cmd)
        {
            SqlCommandSetReflection._Append_SqlCommand.InvokeVoid(this.instance, cmd);
        }

        public void Clear()
        {
            SqlCommandSetReflection._Clear.InvokeVoid(this.instance);
        }

        public int ExecuteNonQuery()
        {
            return SqlCommandSetReflection._ExecuteNonQuery.InvokeDisallowNull<int>(this.instance);
        }

        public int GetParameterCount(int commandIndex)
        {
            return SqlCommandSetReflection._GetParameterCount_Int32.InvokeDisallowNull<int>(this.instance, commandIndex);
        }

        public SqlParameter GetParameter(int commandIndex, int parameterIndex)
        {
            return SqlCommandSetReflection._GetParameter_Int32_Int32.InvokeAllowNull<SqlParameter>(this.instance, commandIndex, parameterIndex);
        }

        public bool GetBatchedAffected(int commandIdentifier, out int recordsAffected, out Exception error)
        {
            object[] args = new object[]
            {
                /* 0: commandIdentifier:*/ commandIdentifier,
                /* 1: recordsAffected  :*/ null,
                /* 2: error            :*/ null,
            };

            bool result = SqlCommandSetReflection._GetBatchedAffected_Int32_Int32_Exception.InvokeDisallowNull<bool>(this.instance, args: args);

            recordsAffected =     (Int32)args[1];
            error           = (Exception)args[2];

            return result;
        }

        public void Dispose()
        {
            // Note that `Microsoft.Data.SqlClient.SqlCommandSet` does NOT nominally implement IDisposable!
            SqlCommandSetReflection._Dispose.InvokeVoid(this.instance);
        }

        //

        public Task<int> ExecuteNonQueryAsync()
        {
            throw new NotImplementedException();
        }
    }
}
