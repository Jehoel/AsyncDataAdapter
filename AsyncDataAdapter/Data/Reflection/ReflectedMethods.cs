using System;
using System.Reflection;

namespace AsyncDataAdapter.Internal
{
    public static class ReflectedAction<TOwner,TName>
        where TName : struct
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireVoidInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), parameterTypes: Array.Empty<Type>() );

        public static void Invoke( TOwner instance )
        {
            Object[] arguments = Array.Empty<Object>();
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            Reflection.AssertVoid( _methodInfo, value );
        }
    }

    public static class ReflectedFunc<TOwner,TName,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), parameterTypes: Array.Empty<Type>() );

        public static TReturn Invoke( TOwner instance )
        {
            Object[] arguments = Array.Empty<Object>();
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            return Reflection.AssertResult<TReturn>( _methodInfo, value );
        }
    }

    public static class ReflectedAction<TOwner,TName,TArg0>
        where TName : struct
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireVoidInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), typeof(TArg0) );

        public static void Invoke( TOwner instance, TArg0 arg0 )
        {
            Object[] arguments = new Object[] { arg0 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            Reflection.AssertVoid( _methodInfo, value );
        }
    }

    public static class ReflectedFunc<TOwner,TName,TArg0,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), typeof(TArg0) );

        public static TReturn Invoke( TOwner instance, TArg0 arg0 )
        {
            Object[] arguments = new Object[] { arg0 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            return Reflection.AssertResult<TReturn>( _methodInfo, value );
        }
    }

    public static class ReflectedAction<TOwner,TName,TArg0,TArg1>
        where TName : struct
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireVoidInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), typeof(TArg0), typeof(TArg1) );

        public static void Invoke( TOwner instance, TArg0 arg0, TArg1 arg1 )
        {
            Object[] arguments = new Object[] { arg0, arg1 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            Reflection.AssertVoid( _methodInfo, value );
        }
    }

    public static class ReflectedFunc<TOwner,TName,TArg0,TArg1,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), typeof(TArg0), typeof(TArg1) );

        public static TReturn Invoke( TOwner instance, TArg0 arg0, TArg1 arg1 )
        {
            Object[] arguments = new Object[] { arg0, arg1 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            return Reflection.AssertResult<TReturn>( _methodInfo, value );
        }
    }

    public static class ReflectedAction<TOwner,TName,TArg0,TArg1,TArg2>
        where TName : struct
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireVoidInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), typeof(TArg0), typeof(TArg1), typeof(TArg2) );

        public static void Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            Reflection.AssertVoid( _methodInfo, value );
        }
    }

    public static class ReflectedFunc<TOwner,TName,TArg0,TArg1,TArg2,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), typeof(TArg0), typeof(TArg1), typeof(TArg2) );

        public static TReturn Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            return Reflection.AssertResult<TReturn>( _methodInfo, value );
        }
    }

    public static class ReflectedAction<TOwner,TName,TArg0,TArg1,TArg2,TArg3>
        where TName : struct
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireVoidInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), typeof(TArg0), typeof(TArg1), typeof(TArg2), typeof(TArg3) );

        public static void Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2, TArg3 arg3 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2, arg3 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            Reflection.AssertVoid( _methodInfo, value );
        }
    }

    public static class ReflectedFunc<TOwner,TName,TArg0,TArg1,TArg2,TArg3,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), typeof(TArg0), typeof(TArg1), typeof(TArg2), typeof(TArg3) );

        public static TReturn Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2, TArg3 arg3 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2, arg3 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            return Reflection.AssertResult<TReturn>( _methodInfo, value );
        }
    }

    public static class ReflectedAction<TOwner,TName,TArg0,TArg1,TArg2,TArg3,TArg4>
        where TName : struct
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireVoidInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), typeof(TArg0), typeof(TArg1), typeof(TArg2), typeof(TArg3), typeof(TArg4) );

        public static void Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2, TArg3 arg3, TArg4 arg4 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2, arg3, arg4 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            Reflection.AssertVoid( _methodInfo, value );
        }
    }

    public static class ReflectedFunc<TOwner,TName,TArg0,TArg1,TArg2,TArg3,TArg4,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), typeof(TArg0), typeof(TArg1), typeof(TArg2), typeof(TArg3), typeof(TArg4) );

        public static TReturn Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2, TArg3 arg3, TArg4 arg4 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2, arg3, arg4 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            return Reflection.AssertResult<TReturn>( _methodInfo, value );
        }
    }

    public static class ReflectedAction<TOwner,TName,TArg0,TArg1,TArg2,TArg3,TArg4,TArg5>
        where TName : struct
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireVoidInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), typeof(TArg0), typeof(TArg1), typeof(TArg2), typeof(TArg3), typeof(TArg4), typeof(TArg5) );

        public static void Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2, TArg3 arg3, TArg4 arg4, TArg5 arg5 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2, arg3, arg4, arg5 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            Reflection.AssertVoid( _methodInfo, value );
        }
    }

    public static class ReflectedFunc<TOwner,TName,TArg0,TArg1,TArg2,TArg3,TArg4,TArg5,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), typeof(TArg0), typeof(TArg1), typeof(TArg2), typeof(TArg3), typeof(TArg4), typeof(TArg5) );

        public static TReturn Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2, TArg3 arg3, TArg4 arg4, TArg5 arg5 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2, arg3, arg4, arg5 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            return Reflection.AssertResult<TReturn>( _methodInfo, value );
        }
    }

    public static class ReflectedAction<TOwner,TName,TArg0,TArg1,TArg2,TArg3,TArg4,TArg5,TArg6>
        where TName : struct
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireVoidInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), typeof(TArg0), typeof(TArg1), typeof(TArg2), typeof(TArg3), typeof(TArg4), typeof(TArg5), typeof(TArg6) );

        public static void Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2, TArg3 arg3, TArg4 arg4, TArg5 arg5, TArg6 arg6 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2, arg3, arg4, arg5, arg6 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            Reflection.AssertVoid( _methodInfo, value );
        }
    }

    public static class ReflectedFunc<TOwner,TName,TArg0,TArg1,TArg2,TArg3,TArg4,TArg5,TArg6,TReturn>
    {
        private static readonly MethodInfo _methodInfo = Reflection.RequireInstanceMethodInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TReturn), typeof(TArg0), typeof(TArg1), typeof(TArg2), typeof(TArg3), typeof(TArg4), typeof(TArg5), typeof(TArg6) );

        public static TReturn Invoke( TOwner instance, TArg0 arg0, TArg1 arg1, TArg2 arg2, TArg3 arg3, TArg4 arg4, TArg5 arg5, TArg6 arg6 )
        {
            Object[] arguments = new Object[] { arg0, arg1, arg2, arg3, arg4, arg5, arg6 };
            Object value = _methodInfo.Invoke( obj: instance, parameters: arguments );
            return Reflection.AssertResult<TReturn>( _methodInfo, value );
        }
    }

} // namespaace
