using System;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.Internal
{
    using IList = System.Collections.IList;

    public struct SqlCommandWrapper
    {
        private readonly SqlCommand cmd;

        public SqlCommandWrapper( SqlCommand cmd )
        {
            this.cmd = cmd ?? throw new ArgumentNullException(nameof(cmd));
        }

        public Boolean BatchRPCMode
        {
            get => SqlCommandReflection._BatchRPCMode_Get.InvokeDisallowNull<bool>( this.cmd );
            set => SqlCommandReflection._BatchRPCMode_Get.InvokeVoid( this.cmd );
        }

        public void ClearBatchCommand()
        {
            SqlCommandReflection._ClearBatchCommand.InvokeVoid( this.cmd );
        }

        public void AddBatchCommand(string commandText, SqlParameterCollection parameters, System.Data.CommandType cmdType, SqlCommandColumnEncryptionSetting columnEncryptionSetting)
        {
            SqlCommandReflection._AddBatchCommand.InvokeVoid( this.cmd, commandText, parameters, cmdType, columnEncryptionSetting );
        }

        /// <summary>Exposes the <c>int _currentlyExecutingBatch</c> field.</summary>
        public Int32 CurrentlyExecutingBatch
        {
            get => SqlCommandReflection._CurrentlyExecutingBatch.GetValueDisallowNull<Int32>(this.cmd);
            set => SqlCommandReflection._CurrentlyExecutingBatch.SetValue(this.cmd, value);
        }

        /// <summary>Exposes the <c>_SqlRPC[] _SqlRPCBatchArray</c> field.</summary>
        public Array SqlRpcBatchArray
        {
            get => SqlCommandReflection._CurrentlyExecutingBatch.GetValueAllowNull<Array>(this.cmd);
            set => SqlCommandReflection._CurrentlyExecutingBatch.SetValue(this.cmd, value);
        }

        /// <summary>Exposes the <c>List&lt;_SqlRPC&gt; _RPCList</c> field.</summary>
        public IList SqlRpcList
        {
            get => SqlCommandReflection._CurrentlyExecutingBatch.GetValueAllowNull<IList>(this.cmd);
            set => SqlCommandReflection._CurrentlyExecutingBatch.SetValue(this.cmd, value);
        }

        public Array SqlRpcListToArray()
        {
            IList asIList = this.SqlRpcList;

            Array newArray = SqlCommandReflection.CreateSqlRpcArray( length: asIList.Count );
            asIList.CopyTo( newArray, index: 0 );
            return newArray;
        }

        public SqlParameterCollection Parameters => this.cmd.Parameters;

        public async Task<Int32> ExecuteBatchRPCCommandAsync( CancellationToken cancellationToken )
        {
            this.SqlRpcBatchArray = this.SqlRpcListToArray();
            this.CurrentlyExecutingBatch = 0;

            Int32 result = await this.cmd.ExecuteNonQueryAsync( cancellationToken ).ConfigureAwait(false);
            return result;
        }
    }
}
