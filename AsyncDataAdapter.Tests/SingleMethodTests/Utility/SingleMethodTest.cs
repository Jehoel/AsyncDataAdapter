using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

using AsyncDataAdapter.Tests.FakeDb;

using NUnit.Framework;

namespace AsyncDataAdapter.Tests.Big3
{
    public abstract class SingleMethodTest<TResult>
    {
        protected abstract TResult RunDbDataAdapterSynchronous( List<TestTable> randomDataSource, FakeDbDataAdapter adapter );

        protected abstract TResult RunProxiedDbDataAdapter( List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter );

        protected abstract Task<TResult> RunProxiedDbDataAdapterAsync( List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter );

        protected abstract Task<TResult> RunBatchingProxiedDbDataAdapterAsync( List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter );
        
        protected abstract void AssertResult( TResult dbSynchronous, TResult dbProxied, TResult dbProxiedAsync, TResult dbBatchingProxiedAsync );

        //

        [Test]
        public virtual async Task RunAsync()
        {
            await this.RunAsync( seed: 1234, tableCount: 5 );
        }

        protected async Task RunAsync( Int32 seed, Int32 tableCount )
        {
            Stopwatch sw = Stopwatch.StartNew();
            List<(TimeSpan,String)> list = new List<(TimeSpan, string)>();

            TResult dbSynchronous = this.DoRunDbDataAdapterSynchronous( seed, tableCount );

        list.Add( ( sw.Elapsed, nameof(this.DoRunDbDataAdapterSynchronous) + " completed" ) );

            TResult dbProxied = this.DoRunProxiedDbDataAdapter( seed, tableCount );

        list.Add( ( sw.Elapsed, nameof(this.DoRunProxiedDbDataAdapter) + " completed" ) );

            TResult dbProxiedAsync = await this.DoRunProxiedDbDataAdapterAsync( seed, tableCount );

        list.Add( ( sw.Elapsed, nameof(this.DoRunProxiedDbDataAdapterAsync) + " completed" ) );
            
            TResult dbBatchingProxiedAsync = await this.DoRunBatchingProxiedDbDataAdapterAsync( seed, tableCount );

        list.Add( ( sw.Elapsed, nameof(this.DoRunBatchingProxiedDbDataAdapterAsync) + " completed" ) );

            //

            this.AssertResult( dbSynchronous, dbProxied, dbProxiedAsync, dbBatchingProxiedAsync );
        }

        protected TResult DoRunDbDataAdapterSynchronous( Int32 seed, Int32 tableCount )
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: seed, tableCount: tableCount );

            using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
            using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
            {
                connection.Open();

                using( FakeDbDataAdapter adapter = new FakeDbDataAdapter( selectCommand ) )
                {
                    return this.RunDbDataAdapterSynchronous( randomDataSource, adapter );
                }
            }
        }

        protected TResult DoRunProxiedDbDataAdapter( Int32 seed, Int32 tableCount )
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: seed, tableCount: tableCount );

            using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
            using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
            {
                connection.Open();

                using( FakeProxiedDbDataAdapter adapter = new FakeProxiedDbDataAdapter( selectCommand ) )
                {
                    return this.RunProxiedDbDataAdapter( randomDataSource, adapter );
                }
            }
        }

        protected async Task<TResult> DoRunProxiedDbDataAdapterAsync( Int32 seed, Int32 tableCount )
        {
            Stopwatch sw = Stopwatch.StartNew();
            List<(TimeSpan,String)> list = new List<(TimeSpan, string)>();

            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: seed, tableCount: tableCount );

            using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AwaitAsync ) )
            using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
            {
                list.Add( ( sw.Elapsed, "In using" ) );

                await connection.OpenAsync();

                list.Add( ( sw.Elapsed, "OpenAsync completed" ) );

                using( FakeProxiedDbDataAdapter adapter = new FakeProxiedDbDataAdapter( selectCommand ) )
                {
                    var x = await this.RunProxiedDbDataAdapterAsync( randomDataSource, adapter );

                    list.Add( ( sw.Elapsed, "RunProxiedDbDataAdapterAsync completed" ) );

                    return x;
                }
            }
        }

        protected async Task<TResult> DoRunBatchingProxiedDbDataAdapterAsync( Int32 seed, Int32 tableCount )
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: seed, tableCount: tableCount );

            using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AwaitAsync ) )
            using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
            {
                await connection.OpenAsync();

                using( BatchingFakeProxiedDbDataAdapter adapter = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                {
                    return await this.RunBatchingProxiedDbDataAdapterAsync( randomDataSource, adapter );
                }
            }
        }
    }
}
