using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

using NUnit.Framework;

namespace AsyncDataAdapter.Tests
{
    /// <summary>The purpose of these tests is to ensure that <see cref="ProxyDbDataAdapter{TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader}"/> is correctly implemented by using a fake DataAdapter.</summary>
    public class ProxyDataAdapterTests
    {
        
    }

    /// <summary>These tests demonstrate that <see cref="ProxyDbDataAdapter{TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader}"/> forwards all synchronous calls to the underlying <see cref="DbDataAdapter"/>.</summary>
    public class SynchronousProxyDataAdapterTests
    {
        // TODO: Test every overload of Fill, FillSchema, and Update!

        [Test]
        public void Proxy_Fill_should_work_identically_to_DbDataReader_Fill()
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            //

            DataSet dataSet = new DataSet();

            using( FakeDbConnection connection = new FakeDbConnection() )
            using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
            {
                connection   .AsyncMode = AsyncMode.AllowSync;
                selectCommand.AsyncMode = AsyncMode.AllowSync;

                connection.Open();

                using( BatchingFakeProxiedDbDataAdapter adpt = new BatchingFakeProxiedDbDataAdapter( selectCommand ) )
                {
                    Int32 totalRows = adpt.Fill( dataSet );

                    throw new NotImplementedException();
                }
            }
        }

        [Test]
        public void ProxyFillSchema_should_work_identically_to_FillSchema()
        {
            FakeDbConnection connection = new FakeDbConnection();
            FakeDbCommand    selectCmd  = connection.CreateCommand();

            throw new NotImplementedException();
        }

        [Test]
        public void ProxyUpdate_should_work_identically_to_Update()
        {
            FakeDbConnection connection = new FakeDbConnection();
            FakeDbCommand    selectCmd  = connection.CreateCommand();

            throw new NotImplementedException();
        }
    }

    /// <summary>These tests demonstrate that <see cref="ProxyDbDataAdapter{TDbDataAdapter, TDbConnection, TDbCommand, TDbDataReader}"/>'s async methods are truly async with no calls into synchronous code-paths.</summary>
    public class AsynchronousProxyDataAdapterTests
    {
        // TODO: Test every overload of FillAsync, FillSchemaAsync, and UpdateAsync!

        [Test]
        public async Task ProxyFillAsync_should_not_use_synchronous_calls()
        {
            FakeDbConnection connection = new FakeDbConnection();
            FakeDbCommand    selectCmd  = connection.CreateCommand();

            throw new NotImplementedException();
        }

        [Test]
        public async Task ProxyFillSchemaAsync_should_not_use_synchronous_calls()
        {
            FakeDbConnection connection = new FakeDbConnection();
            FakeDbCommand    selectCmd  = connection.CreateCommand();

            throw new NotImplementedException();
        }

        [Test]
        public async Task ProxyUpdateAsync_should_not_use_synchronous_calls()
        {
            FakeDbConnection connection = new FakeDbConnection();
            FakeDbCommand    selectCmd  = connection.CreateCommand();

            throw new NotImplementedException();
        }
    }
}
