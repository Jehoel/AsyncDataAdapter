using System;
using System.Data;
using System.Data.Common;

namespace AsyncDataAdapter.Tests
{
    public class FakeDbCommand : DbCommand
    {
        public FakeDbCommand()
        {
        }

        public FakeDbCommand( FakeDbConnection c )
        {
            base.Connection = c;
        }

        public new FakeDbConnection Connection => (FakeDbConnection)base.Connection;

        public override void Cancel()
        {
        }

        protected override DbParameter CreateDbParameter()
        {
            return new FakeDbParameter();
        }

        public Func<FakeDbCommand,DbDataReader> CreateReader { get; set; } = cmd => new FakeDbDataReader( cmd );

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return this.CreateReader( this );
        }

        public Int32 ExecuteNonQuery_Ret;

        public override Int32 ExecuteNonQuery()
        {
            return this.ExecuteNonQuery_Ret;
        }

        public Object ExecuteScalar_Ret;

        public override Object ExecuteScalar()
        {
            return this.ExecuteScalar_Ret;
        }

        public override void Prepare()
        {
        }

        public    override String                CommandText           { get; set; }
        public    override Int32                 CommandTimeout        { get; set; }
        public    override CommandType           CommandType           { get; set; }
        protected override DbConnection          DbConnection          { get; set; }
        protected override DbTransaction         DbTransaction         { get; set; }
        public    override Boolean               DesignTimeVisible     { get; set; }
        public    override UpdateRowSource       UpdatedRowSource      { get; set; }
        
        protected override DbParameterCollection DbParameterCollection { get; } = new FakeDbParameterCollection();
    }
}
