using System;
using System.Reflection;

namespace AsyncDataAdapter.Internal
{
    public static class ReflectedFuncO2O3<TOwner,TName,TArg0,TArg1,TArg2,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), typeof(TArg0), typeof(TArg1), typeof(TArg2) );

        public static TReturn Invoke( TOwner instance, TArg0 arg0, out TArg1 arg1, out TArg2 arg2 )
        {
            Object[] arguments = new Object[] { arg0, null, null };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            TReturn returnValue = Reflection.AssertResult<TReturn>( _methodInfo, value );
            arg1 = (TArg1)arguments[1];
            arg2 = (TArg2)arguments[2];
            return returnValue;
        }
    }
}
