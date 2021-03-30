using System;
using System.ComponentModel;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncDataAdapter.Internal
{
    // TODO: Many of these methods aren't async and so could use reflection instead of being reimplemented.

    public static class BatchingAdapterMethods
    {
        /// <summary>This is the same logic as SqlDataAdapter.</summary>
        /// <param name="missingMappingAction">Get this value from <see cref="DataAdapter.MissingMappingAction"/>.</param>
        public static MissingMappingAction UpdateMappingAction( MissingMappingAction missingMappingAction )
        {
            if( MissingMappingAction.Passthrough == missingMappingAction )
		    {
			    return MissingMappingAction.Passthrough;
		    }

		    return MissingMappingAction.Error;
        }

        /// <summary>This is the same logic as SqlDataAdapter.</summary>
        /// <param name="missingSchemaAction">Get this value from <see cref="DataAdapter.MissingSchemaAction"/>.</param>
        public static MissingSchemaAction UpdateSchemaAction( MissingSchemaAction missingSchemaAction )
        {
		    if (MissingSchemaAction.Add == missingSchemaAction || MissingSchemaAction.AddWithKey == missingSchemaAction)
		    {
			    return MissingSchemaAction.Ignore;
		    }

		    return MissingSchemaAction.Error;
        }
    }
}
