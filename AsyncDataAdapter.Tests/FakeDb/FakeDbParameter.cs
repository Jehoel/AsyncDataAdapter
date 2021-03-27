using System;
using System.Data;
using System.Data.Common;

namespace AsyncDataAdapter.Tests
{
    public class FakeDbParameter : DbParameter
    {
        public FakeDbParameter()
        {
        }

        public FakeDbParameter( String name, DbType dbType ) 
        {
            this.ParameterName = name;
            this.DbType        = dbType;
        }

        public override void ResetDbType()
        {
            this.DbType = DbType.String;
        }

        public override DbType             DbType                  { get; set; } = DbType.String;
        public override ParameterDirection Direction               { get; set; } = ParameterDirection.Input;
        public override Boolean            IsNullable              { get; set; } = true;
        public override String             ParameterName           { get; set; } = "?";
        public override Int32              Size                    { get; set; }
        public override String             SourceColumn            { get; set; }
        public override Boolean            SourceColumnNullMapping { get; set; }
        public override Object             Value                   { get; set; }
    }
}
