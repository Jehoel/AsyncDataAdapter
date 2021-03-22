using System;

using Microsoft.Data.SqlClient;

namespace AsyncDataAdapter.Internal
{
    using CommandType = System.Data.CommandType;

    public struct LocalCommandWrapper
    {
        private readonly Object instance;

        public LocalCommandWrapper( Object instance )
        {
            this.instance = instance ?? throw new ArgumentNullException(nameof(instance));
        }

        public string CommandText => LocalCommandReflection._CommandText.GetValueAllowNull<String>(this.instance);

        public SqlParameterCollection Parameters => LocalCommandReflection._Parameters.GetValueAllowNull<SqlParameterCollection>(this.instance);

        public CommandType CmdType => LocalCommandReflection._CmdType.GetValueDisallowNull<CommandType>(this.instance);

        public SqlCommandColumnEncryptionSetting ColumnEncryptionSetting => LocalCommandReflection._ColumnEncryptionSetting.GetValueDisallowNull<SqlCommandColumnEncryptionSetting>(this.instance);
    }
}
