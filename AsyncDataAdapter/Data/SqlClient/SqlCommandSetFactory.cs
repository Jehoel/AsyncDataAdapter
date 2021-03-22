using System;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter.SqlClient
{
    public static class SqlCommandSetFactory
    {
        /// <summary>Set this property to a custom method that instantiates an application-defined implementation of <see cref="ISqlCommandSet"/>.</summary>
        public static Func<ISqlCommandSet> CustomFactory { get; }

        public static ISqlCommandSet CreateInstance()
        {
            Func<ISqlCommandSet> customFactory = SqlCommandSetFactory.CustomFactory;
            if( customFactory != null )
            {
                ISqlCommandSet customInstance = customFactory();
                if( customInstance is null ) throw new InvalidOperationException( "The custom " + nameof(ISqlCommandSet) + " factory returned a null reference." );
                return customInstance;
            }

            try
            {
                Object instance = SqlCommandSetReflection.CreateInstance();
                return new SqlCommandSetWrapper( instance );
            }
            catch( TypeLoadException tlEx )
            {
                const string msg = "The built-in SqlCommandSet is not available for the current platform, but no alternative " + nameof(ISqlCommandSet) + " implementation factory has been provided.";
                throw new PlatformNotSupportedException(message: msg, inner: tlEx);
            }
        }
    }
}
