using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Data.Common;

namespace AsyncDataAdapter.Tests.FakeDb
{
    public class FakeDbParameterCollection : DbParameterCollection
    {
        private readonly Object listLock = new Object();
        private readonly List<FakeDbParameter> list = new List<FakeDbParameter>();

        public override int Add(object value)
        {
            if( value is FakeDbParameter p )
            {
                this.list.Add( p );
                return this.list.Count;
            }
            else
            {
                throw new ArgumentException( "Argument is null or is the wrong type." );
            }
        }

        public override void AddRange(Array values)
        {
            foreach( Object obj in values )
            {
                _ = this.Add( obj );
            }
        }

        public override void Clear()
        {
            this.list.Clear();
        }

        public override bool Contains(object value)
        {
            if( value is FakeDbParameter p )
            {
                return this.list.Contains( p );
            }
            else
            {
                return false;
            }
        }

        public override bool Contains(string value)
        {
            return this.list.Any( p => p.ParameterName == value );
        }

        public override void CopyTo(Array array, int index)
        {
            FakeDbParameter[] a2 = (FakeDbParameter[])array;

            this.list.CopyTo( a2, index );
        }

        public override IEnumerator GetEnumerator()
        {
            return this.list.GetEnumerator();
        }

        protected override DbParameter GetParameter(int index)
        {
            return this.list[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return this.list.SingleOrDefault( p => p.ParameterName == parameterName );
        }

        public override int IndexOf(object value)
        {
            if( value is FakeDbParameter p )
            {
                return this.list.IndexOf( p );
            }

            return -1;
        }

        public override int IndexOf(string parameterName)
        {
            var match = this.list
                .Select( ( p, idx ) => ( p, idx ) )
                .SingleOrDefault( t => t.p.ParameterName == parameterName );

            if( match != default )
            {
                return match.idx;
            }

            return -1;
        }

        public override void Insert(int index, object value)
        {
            if( value is FakeDbParameter p )
            {
                this.list.Insert( index, p );
            }
            else
            {
                throw new ArgumentException( "Null or incorrect parameter type." );
            }
        }

        public override void Remove(object value)
        {
            if( value is FakeDbParameter p )
            {
                _ = this.list.Remove( p );
            }
            else
            {
                throw new ArgumentException( "Null or incorrect parameter type." );
            }
        }

        public override void RemoveAt(int index)
        {
            this.list.RemoveAt( index );
        }

        public override void RemoveAt(string parameterName)
        {
            var match = this.list
                .Select( ( p, idx ) => ( p, idx ) )
                .SingleOrDefault( t => t.p.ParameterName == parameterName );

            if( match != default )
            {
                this.list.RemoveAt( match.idx );
            }
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            if( value is FakeDbParameter p )
            {
                this.list[ index ] = p;
            }
            else
            {
                throw new ArgumentException( "Null or incorrect parameter type." );
            }
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            if( value is FakeDbParameter p )
            {
                var match = this.list
                    .Select( ( p, idx ) => ( p, idx ) )
                    .SingleOrDefault( t => t.p.ParameterName == parameterName );

                if( match != default )
                {
                    this.list[ match.idx ] = p;
                }
            }
            else
            {
                throw new ArgumentException( "Null or incorrect parameter type." );
            }
        }

        public override int Count => this.list.Count;

        public override object SyncRoot => this.listLock;
    }
}
