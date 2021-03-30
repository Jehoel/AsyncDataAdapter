using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Text;

namespace AsyncDataAdapter
{
    public interface IDbCommandBuilder
    {
        CatalogLocation CatalogLocation  { get; set; }
        string          CatalogSeparator { get; set; }
        ConflictOption  ConflictOption   { get; set; }
        DbDataAdapter   DataAdapter      { get; set; }
        string          QuotePrefix      { get; set; }
        string          QuoteSuffix      { get; set; }
        string          SchemaSeparator  { get; set; }
        bool            SetAllValues     { get; set; }

        DbCommand GetDeleteCommand();
        DbCommand GetDeleteCommand(bool useColumnsForParameterNames);
        DbCommand GetInsertCommand();
        DbCommand GetInsertCommand(bool useColumnsForParameterNames);
        DbCommand GetUpdateCommand();
        DbCommand GetUpdateCommand(bool useColumnsForParameterNames);

        string QuoteIdentifier(string unquotedIdentifier);
        void RefreshSchema();
        string UnquoteIdentifier(string quotedIdentifier);
    }

    public interface IAsyncDbCommandBuilder : IDbCommandBuilder
    {
        new IAsyncDbDataAdapter DataAdapter { get; }
    }
}
