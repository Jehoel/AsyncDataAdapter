using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace AsyncDataAdapter
{
	internal static class Reflection
	{
        public static MethodInfo GetStaticMethod<T>( string name, params Type[] paramTypes )
		{
			Type type = typeof(T);

			MethodInfo methodInfo = type.GetMethod(
				name       : name,
				bindingAttr: BindingFlags.NonPublic | BindingFlags.Static,
				binder     : null,
				types      : paramTypes,
				modifiers  : null
			);

			if (methodInfo is null)
			{
				string msg = string.Format("Couldn't find static method {0} in type {1}, in assembly {2}.", name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}
			else
			{
				return methodInfo;
			}
		}

		public static MethodInfo GetInstanceMethod<T>( string name, params Type[] paramTypes )
		{
			Type type = typeof(T);

			MethodInfo methodInfo = type.GetMethod(
				name       : name,
				bindingAttr: BindingFlags.NonPublic | BindingFlags.Instance,
				binder     : null,
				types      : paramTypes,
				modifiers  : null
			);

			if (methodInfo is null)
			{
				string msg = string.Format("Couldn't find instance method {0} in type {1}, in assembly {2}.", name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}
			else
			{
				return methodInfo;
			}
		}

		public static MethodInfo GetInstancePropertyGetter<T>( string name, Type propertyType )
		{
			Type type = typeof(T);

			PropertyInfo propertyInfo = type.GetProperty(
				name       : name,
				bindingAttr: BindingFlags.NonPublic | BindingFlags.Instance,
				binder     : null,
				returnType : propertyType,
				types      : null,
				modifiers  : null
			);

			if (propertyInfo is null)
			{
				string msg = string.Format("Couldn't find property {0} in type {1}, in assembly {2}.", name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}

			MethodInfo propertyGetterInfo = propertyInfo.GetGetMethod(nonPublic: true);
			if (propertyGetterInfo is null)
			{
				string msg = string.Format("Couldn't find getter for property {0} in type {1}, in assembly {2}.", name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}

			return propertyGetterInfo;
		}

        #region MethodInfo Invoke Wrappers

        public static void InvokeVoid(this MethodInfo methodInfo, object @this, params object[] args)
        {
            object result = methodInfo.Invoke(obj: @this, parameters: args);
            if(result != null)
            {
                string msg = string.Format("Expected {0}.{1} to be void and for MethodInfo.Invoke return a null reference, but encountered {2} instead.", methodInfo.DeclaringType.FullName, methodInfo.Name, result.GetType().AssemblyQualifiedName);
                throw new InvalidOperationException(msg);
            }
        }

        public static TReturn InvokeDisallowNull<TReturn>(this MethodInfo methodInfo, object @this, params object[] args)
        {
            object result = methodInfo.Invoke(obj: @this, parameters: args);
            if (result is TReturn typedReturn)
            {
                return typedReturn;
            }
            else
            {
                if (result is null)
                {
                    string msg = string.Format("Expected {0}.{1} to return a {2} value, but encountered a null reference instead.", methodInfo.DeclaringType.FullName, methodInfo.Name, typeof(TReturn).AssemblyQualifiedName);
                    throw new InvalidOperationException(msg);
                }
                else
                {
                    string msg = string.Format("Expected {0}.{1} to return a {2} value, but encountered a {3} reference instead.", methodInfo.DeclaringType.FullName, methodInfo.Name, typeof(TReturn).AssemblyQualifiedName, result.GetType().AssemblyQualifiedName);
                    throw new InvalidOperationException(msg);
                }
            }
        }

        public static TReturn InvokeAllowNull<TReturn>(this MethodInfo methodInfo, object @this, params object[] args)
            where TReturn : class
        {
            object result = methodInfo.Invoke(obj: @this, parameters: args);
            if (result is TReturn typedReturn)
            {
                return typedReturn;
            }
            else if(result is null)
            {
                return null;
            }
            else
            {
                string msg = string.Format("Expected {0}.{1} to return a {2} value, but encountered a {3} reference instead.", methodInfo.DeclaringType.FullName, methodInfo.Name, typeof(TReturn).AssemblyQualifiedName, result.GetType().AssemblyQualifiedName);
                throw new InvalidOperationException(msg);
            }
        }

        #endregion
    }
}
