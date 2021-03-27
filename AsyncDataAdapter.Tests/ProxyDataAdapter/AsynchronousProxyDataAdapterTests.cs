using System;
using System.Threading.Tasks;

using NUnit.Framework;

namespace AsyncDataAdapter.Tests
{
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
