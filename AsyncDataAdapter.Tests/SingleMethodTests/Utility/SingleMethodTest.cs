using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using AsyncDataAdapter.Tests.FakeDb;

namespace AsyncDataAdapter.Tests
{
    public abstract class SingleMethodTest<TResult>
    {
        protected abstract TResult RunDbDataAdapterSynchronous( List<TestTable> randomDataSource, FakeDbDataAdapter adapter );

        protected abstract TResult RunProxiedDbDataAdapter( List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter );

        protected abstract Task<TResult> RunProxiedDbDataAdapterAsync( List<TestTable> randomDataSource, FakeProxiedDbDataAdapter adapter );

        protected abstract Task<TResult> RunBatchingProxiedDbDataAdapterAsync( List<TestTable> randomDataSource, BatchingFakeProxiedDbDataAdapter adapter );
        
        protected abstract void AssertResult( TResult dbSynchronous, TResult dbProxied, TResult dbProxiedAsync, TResult dbBatchingProxiedAsync );

        //

        protected async Task RunAsync( Int32 seed, Int32 tableCount )
        {
            TResult dbSynchronous = this.DoRunDbDataAdapterSynchronous( seed, tableCount );

            TResult dbProxied = this.DoRunProxiedDbDataAdapter( seed, tableCount );

            TResult dbProxiedAsync = await this.DoRunProxiedDbDataAdapterAsync( seed, tableCount );

            TResult dbBatchingProxiedAsync = await this.DoRunBatchingProxiedDbDataAdapterAsync( seed, tableCount );

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
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: seed, tableCount: tableCount );

            using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AwaitAsync ) )
            using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
            {
                await connection.OpenAsync();

                using( FakeProxiedDbDataAdapter adapter = new FakeProxiedDbDataAdapter( selectCommand ) )
                {
                    return await this.RunProxiedDbDataAdapterAsync( randomDataSource, adapter );
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
