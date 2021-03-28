using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;

namespace AsyncDataAdapter.Internal
{
    public static class ReflectedProperty<TOwner,TName,TProperty>
        where TName : struct
    {
        private static readonly PropertyInfo _propertyInfo = Reflection.RequireInstancePropertyInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), returnType: typeof(TProperty) );
        private static readonly MethodInfo   _getter       = _propertyInfo.GetGetMethod( nonPublic: true );
        private static readonly MethodInfo   _setter       = _propertyInfo.GetSetMethod( nonPublic: true );

        public static TProperty GetValue( TOwner instance )
        {
            Object value = _getter.Invoke( obj: instance, parameters: null );
            return Reflection.AssertResult<TProperty>( _propertyInfo, value );
        }

        public static void SetValue( TOwner instance, TProperty value )
        {
            _ = _setter.Invoke( instance, new Object[] { value } );
        }
    }

    public struct ReflectedField<TOwner,TName,TField>
        where TName : struct
    {
        private static readonly FieldInfo _fieldInfo = Reflection.RequireInstanceFieldInfo( typeof(TOwner), name: Reflection.GetName(typeof(TName)), fieldType: typeof(TField) );

        public static TField GetValue( TOwner instance )
        {
            Object value = _fieldInfo.GetValue( obj: instance );
            return Reflection.AssertResult<TField>( _fieldInfo, value );
        }

        public static void SetValue( TOwner instance, TField value )
        {
            _fieldInfo.SetValue( instance, value );
        }
    }
}
