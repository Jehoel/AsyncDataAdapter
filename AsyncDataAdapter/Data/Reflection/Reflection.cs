using System;
using System.Reflection;

namespace AsyncDataAdapter
{
    internal static class Reflection
	{
        public static MethodInfo GetStaticMethod<T>( string name, params Type[] paramTypes )
		{
            return GetNonPublicMethod(type: typeof(T), name: name, bindingAttr: BindingFlags.Static, paramTypes: paramTypes);
        }

        public static MethodInfo GetStaticMethod(Type type, string name, params Type[] paramTypes )
		{
            return GetNonPublicMethod(type: type, name: name, bindingAttr: BindingFlags.Static, paramTypes: paramTypes);
        }

		public static MethodInfo GetInstanceMethod<T>( string name, params Type[] paramTypes )
		{
			return GetInstanceMethod(type: typeof(T), name: name, paramTypes: paramTypes);
		}

        public static MethodInfo GetInstanceMethod(Type type, string name, params Type[] paramTypes)
		{
			return GetNonPublicMethod(type: type, name: name, bindingAttr: BindingFlags.Instance, paramTypes: paramTypes);
		}

        private static MethodInfo GetNonPublicMethod(Type type, string name, BindingFlags bindingAttr, Type[] paramTypes)
		{
			MethodInfo methodInfo = type.GetMethod(
				name       : name,
				bindingAttr: BindingFlags.NonPublic | bindingAttr,
				binder     : null,
				types      : paramTypes,
				modifiers  : null
			);

			if (methodInfo is null)
			{
				string msg = string.Format("Couldn't find {0} method {1} in type {2}.", bindingAttr.ToString(), name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}
			else
			{
				return methodInfo;
			}
		}

        public static MethodInfo GetInstancePropertyGetter<T>( string name, Type propertyType )
		{
            return GetInstancePropertyGetter(typeof(T), name, propertyType);
        }

		public static MethodInfo GetInstancePropertyGetter(Type type, string name, Type propertyType)
		{
			PropertyInfo propertyInfo = GetInstancePropertyInfo(type, name, propertyType);

			MethodInfo propertyGetterInfo = propertyInfo.GetGetMethod(nonPublic: true);
			if (propertyGetterInfo is null)
			{
				string msg = string.Format("Couldn't find getter for property {0} in type {1}.", name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}
            else
            {
                return propertyGetterInfo;
            }
		}

        public static MethodInfo GetInstancePropertySetter(Type type, string name, Type propertyType)
		{
            PropertyInfo propertyInfo = GetInstancePropertyInfo(type, name, propertyType);

			MethodInfo propertySetterInfo = propertyInfo.GetSetMethod(nonPublic: true);
			if (propertySetterInfo is null)
			{
				string msg = string.Format("Couldn't find setter for property {0} in type {1}.", name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}
            else
            {
                return propertySetterInfo;
            }
        }

        private static PropertyInfo GetInstancePropertyInfo(Type type, string name, Type propertyType)
        {
            PropertyInfo propertyInfo = type.GetProperty(
				name       : name,
				bindingAttr: BindingFlags.NonPublic | BindingFlags.Instance,
				binder     : null,
				returnType : propertyType,
				types      : Array.Empty<Type>(),
				modifiers  : null
			);

			if (propertyInfo is null)
			{
				string msg = string.Format("Couldn't find property {0} in type {1}.", name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}
            else
            {
                return propertyInfo;
            }
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
