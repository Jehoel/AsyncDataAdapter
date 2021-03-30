using System;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace AsyncDataAdapter.Tests.FakeDb
{
    public class FakeDbCommandBuilder : DbCommandBuilder
    {
        /// <summary>This ctor is only used by <see cref="FakeDbProviderFactory"/> and should not be called by anyone, ever, really.</summary>
        internal FakeDbCommandBuilder()
        {
            base.QuotePrefix = "[";
            base.QuoteSuffix = "]";
        }

        public FakeDbCommandBuilder( FakeDbDataAdapter adapter )
            : base()
        {
            base.QuotePrefix = "[";
            base.QuoteSuffix = "]";
            this.DataAdapter = adapter;
        }

        protected override void ApplyParameterInfo( DbParameter parameter, DataRow row, StatementType statementType, bool whereClause )
        {
            // NOOP.
        }

        protected override string GetParameterName(int parameterOrdinal)
        {
            return "@p" + parameterOrdinal.ToString( CultureInfo.InvariantCulture );
        }

        protected override string GetParameterName(string parameterName)
        {
            return "@" + parameterName;
        }

        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            return "@p" + parameterOrdinal.ToString(CultureInfo.InvariantCulture);
        }

        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            FakeDbDataAdapter fda = (FakeDbDataAdapter)adapter;

            if (fda == base.DataAdapter)
            {
//             fda.RowUpdating -= this.RowUpdatingHandler;
            }
            else
            {
//             fda.RowUpdating += this.RowUpdatingHandler;
            }
        }

        // Why aren't these methods abstract? The default impl always throws.
        public override string QuoteIdentifier(string unquotedIdentifier)
        {
            if( String.IsNullOrWhiteSpace( unquotedIdentifier ) ) return unquotedIdentifier;

            return '[' + unquotedIdentifier.Trim( '[', ']' ) + ']';
        }

        public override string UnquoteIdentifier(string quotedIdentifier)
        {
            if( String.IsNullOrWhiteSpace( quotedIdentifier ) ) return quotedIdentifier;

            return quotedIdentifier.Trim( '[', ']' );
        }

        //

        public new FakeDbCommand GetUpdateCommand() => (FakeDbCommand)base.GetUpdateCommand();

        public new FakeDbCommand GetUpdateCommand( Boolean useColumnsForParameterNames ) => (FakeDbCommand)base.GetUpdateCommand( useColumnsForParameterNames );

        //

        public new FakeDbCommand GetDeleteCommand() => (FakeDbCommand)base.GetDeleteCommand();

        public new FakeDbCommand GetDeleteCommand( Boolean useColumnsForParameterNames ) => (FakeDbCommand)base.GetDeleteCommand( useColumnsForParameterNames );

        //

        public new FakeDbCommand GetInsertCommand() => (FakeDbCommand)base.GetInsertCommand();

        public new FakeDbCommand GetInsertCommand( Boolean useColumnsForParameterNames ) => (FakeDbCommand)base.GetInsertCommand( useColumnsForParameterNames );
    }
}
