using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Shouldly;

namespace AsyncDataAdapter.Tests
{
    /// <summary>Just some meta-tests so I know my FakeDb stuff works.</summary>
    public class RandomDataTests
    {
        [Test]
        public void FakeDbDataReader_should_behave()
        {
            FakeDbCommand cmd = new FakeDbCommand();

            FakeDbDataReader rdr = new FakeDbDataReader( cmd );

            //

            List<TestTable> tables = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5 );

            rdr.ResetAndLoadTestData( tables );

            rdr.AllTables.Count.ShouldBe( 5 );
        }
    }
}
