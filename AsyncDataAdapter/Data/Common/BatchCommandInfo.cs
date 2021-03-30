using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter.Internal
{
    public struct BatchCommandInfo
    {
        public int             CommandIdentifier;     // whatever AddToBatch returns, so we can reference the command later in GetBatchedParameter
        public int             ParameterCount;        // number of parameters on the command, so we know how many to loop over when processing output parameters
        public DataRow         Row;                   // the row that the command is intended to update
        public StatementType   StatementType;         // the statement type of the command, needed for accept changes
        public UpdateRowSource UpdatedRowSource;      // the UpdatedRowSource value from the command, to know whether we need to look for output parameters or not
        public int?            RecordsAffected;
        public Exception       Errors;
    }
}
