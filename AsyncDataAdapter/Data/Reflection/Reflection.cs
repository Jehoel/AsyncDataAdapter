using System;
using System.Globalization;
using System.Reflection;

namespace AsyncDataAdapter.Internal
{
    public static class Reflection
	{
        #region MethodInfo

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

        #endregion

        #region PropertyInfo

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

        public static MethodInfo GetInstancePropertySetter<T>(string name, Type propertyType)
		{
            return GetInstancePropertySetter(typeof(T), name, propertyType);
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

        #endregion

        #region FieldInfo

        public static Boolean TryGetStaticFieldInfo( Type type, string name, out FieldInfo fieldInfo )
        {
            fieldInfo = type.GetField( name, BindingFlags.NonPublic | BindingFlags.Static );
            if (fieldInfo is null)
			{
				return false;
			}
            else
            {
                return true;
            }
        }

        public static FieldInfo GetInstanceFieldInfo<T>( string name, Type fieldType )
        {
            return GetInstanceFieldInfo( typeof(T), name, fieldType );
        }

        public static FieldInfo GetInstanceFieldInfo( Type type, string name, Type fieldType )
        {
            FieldInfo fieldInfo = type.GetField( name, BindingFlags.NonPublic | BindingFlags.Instance );
            if (fieldInfo is null)
			{
				string msg = string.Format("Couldn't find field {0} in type {1}.", name, type.AssemblyQualifiedName);
				throw new InvalidOperationException(msg);
			}
            else
            {
                if( !fieldType.IsAssignableFrom( fieldInfo.FieldType ) )
                {
                    string msg = String.Format( CultureInfo.CurrentCulture, "Expected reflected FieldInfo for {0}::{1} to have field type {2} but the FieldInfo has field type {3}.", type.FullName, name, fieldType.FullName, fieldInfo.FieldType.FullName );
                    throw new InvalidOperationException( message: msg );
                }
                else
                {
                    return fieldInfo;
                }
            }
        }

        #endregion

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

        #region FieldInfo

        public static T GetValueDisallowNull<T>(this FieldInfo fieldInfo, object @this)
        {
            Object value = fieldInfo.GetValue(obj: @this);
            if( value is T typed )
            {
                return typed;
            }
            else
            {
                if (value is null)
                {
                    string msg = string.Format("Expected {0}.{1} to hold a {2} value, but encountered a null reference instead.", fieldInfo.DeclaringType.FullName, fieldInfo.Name, typeof(T).AssemblyQualifiedName);
                    throw new InvalidOperationException(msg);
                }
                else
                {
                    string msg = string.Format("Expected {0}.{1} to hold a {2} value, but encountered a {3} reference instead.", fieldInfo.DeclaringType.FullName, fieldInfo.Name, typeof(T).AssemblyQualifiedName, value.GetType().AssemblyQualifiedName);
                    throw new InvalidOperationException(msg);
                }
            }
        }

        public static T GetValueAllowNull<T>(this FieldInfo fieldInfo, object @this)
            where T : class
        {
            Object value = fieldInfo.GetValue(obj: @this);
            if( value is T typed )
            {
                return typed;
            }
            else if( value is null )
            {
                return null;
            }
            else
            {
                string msg = string.Format("Expected {0}.{1} to hold a {2} value, but encountered a {3} reference instead.", fieldInfo.DeclaringType.FullName, fieldInfo.Name, typeof(T).AssemblyQualifiedName, value.GetType().AssemblyQualifiedName);
                throw new InvalidOperationException(msg);
            }
        }

//      public static void SetValue<T>(this FieldInfo fieldInfo, object @this, T newValue)
//      {
//          fieldInfo.SetValue(obj: @this, value: newValue);
//      }

        #endregion

        public static String GetName( Type nameType )
        {
            String name = nameType.Name;
            return name.Substring( startIndex: 1 ); // TODO: Perhaps requires a known prefix?
        }

        public static FieldInfo RequireInstanceFieldInfo( Type owner, String name, Type fieldType )
        {
            const BindingFlags bf = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;

            FieldInfo fieldInfo = owner.GetField( name: name, bindingAttr: bf );
            if( fieldInfo is null )
            {
                throw new InvalidOperationException( message: "Could not reflect FieldInfo for " + owner.FullName + "::" + name );
            }
            else
            {
                if( !fieldType.IsAssignableFrom( fieldInfo.FieldType ) )
                {
                    string msg = String.Format( CultureInfo.CurrentCulture, "Expected reflected FieldInfo for {0}::{1} to have field type {2} but the FieldInfo has field type {3}.", owner.FullName, name, fieldType.FullName, fieldInfo.FieldType.FullName );
                    throw new InvalidOperationException( message: msg );
                }
                else
                {
                    return fieldInfo;
                }
            }
        }

        public static PropertyInfo RequireInstancePropertyInfo( Type owner, String name, Type returnType )
        {
            const BindingFlags bf = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;

            PropertyInfo methodInfo = owner.GetProperty( name: name, bindingAttr: bf, binder: null, returnType: returnType, types: null, modifiers: null );
            if( methodInfo is null )
            {
                string msg = String.Format( CultureInfo.CurrentCulture, "Could not reflect PropertyInfo for {0}::{1} with property type {2}.", owner.FullName, name, returnType.FullName );
                throw new InvalidOperationException( message: msg );
            }
            else
            {
                return methodInfo;
            }
        }

        public static MethodInfo RequireInstanceMethodInfo( Type owner, String name, Type returnType, params Type[] parameterTypes )
        {
            const BindingFlags bf = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;

            MethodInfo methodInfo = owner.GetMethod( name: name, bindingAttr: bf, binder: null, types: parameterTypes, modifiers: null );
            if( methodInfo is null )
            {
                throw new InvalidOperationException( message: "Could not reflect MethodInfo for " + owner.FullName + "::" + name );
            }
            else
            {
                if( !returnType.IsAssignableFrom( methodInfo.ReturnType ) )
                {
                    string msg = String.Format( CultureInfo.CurrentCulture, "Expected reflected MethodInfo for {0}::{1} to have return type {2} but the MethodInfo has return type {3}.", owner.FullName, name, returnType.FullName, methodInfo.ReturnType.FullName );
                    throw new InvalidOperationException( message: msg );
                }
                else
                {
                    return methodInfo;
                }
            }
        }

        public static MethodInfo RequireVoidInstanceMethodInfo( Type owner, String name, params Type[] parameterTypes )
        {
            const BindingFlags bf = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;

            MethodInfo methodInfo = owner.GetMethod( name: name, bindingAttr: bf, binder: null, types: parameterTypes, modifiers: null );
            if( methodInfo is null )
            {
                throw new InvalidOperationException( message: "Could not reflect MethodInfo for " + owner.FullName + "::" + name );
            }
            else
            {
                if( methodInfo.ReturnType is null || methodInfo.ReturnType != typeof(void) )
                {
                    string msg = String.Format( CultureInfo.CurrentCulture, "Expected reflected MethodInfo for {0}::{1} to have void return type but the MethodInfo has return type {2}.", owner.FullName, name, methodInfo.ReturnType.FullName );
                    throw new InvalidOperationException( message: msg );
                }
                else
                {
                    return methodInfo;
                }
            }
        }

        public static void AssertVoid( MemberInfo memberInfo, Object value )
        {
            if( !( value is null ) )
            {
                string msg = String.Format( CultureInfo.CurrentCulture, "Expected {0}.{1} to return void, but encountered {2} instead.", memberInfo.DeclaringType.FullName, memberInfo.Name, value.GetType().FullName );
                throw new InvalidOperationException( msg );
            }
        }

        public static TReturn AssertResult<TReturn>( MemberInfo memberInfo, Object value, Boolean allowNull = true )
        {
            if( value is TReturn typed )
            {
                return typed;
            }
            else if( value is null )
            {
                if( allowNull )
                {
                    return default;
                }
                else
                {
                    string msg = String.Format( CultureInfo.CurrentCulture, "Expected {0}.{1} not to return null, but encountered a null return value.", memberInfo.DeclaringType.FullName, memberInfo.Name );
                    throw new InvalidOperationException( msg );
                }
            }
            else
            {
                string msg = String.Format( CultureInfo.CurrentCulture, "Expected {0}.{1} to return {2}, but encountered {3} instead.", memberInfo.DeclaringType.FullName, memberInfo.Name, typeof(TReturn).FullName, value.GetType().FullName );
                throw new InvalidOperationException( msg );
            }
        }
    }
}
