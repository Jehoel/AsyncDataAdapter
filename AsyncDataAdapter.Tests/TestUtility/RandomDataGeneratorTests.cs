using System;
using System.Collections.Generic;

using NUnit.Framework;

using Shouldly;

namespace AsyncDataAdapter.Tests.MetaTests
{
    /// <summary>Just some meta-tests so I know my <see cref="RandomDataGenerator"/> class works.</summary>
    public class RandomDataGeneratorTests
    {
        [Test]
        [TestCase( new Object[] { /*seed:*/ 1234, /*table row counts: */ new Int32[] { 40, 68, 35, 65, 76 } } )]
        public void RandomDataGenerator_seed_values_should_produce_expected_results( Int32 seed, Int32[] rowCounts )
        {
            List<TestTable> tables = RandomDataGenerator.CreateRandomTables( seed: seed, tableCount: rowCounts.Length );

            tables.Count.ShouldBe( rowCounts.Length );

            for( Int32 i = 0; i < rowCounts.Length; i++ )
            {
                tables[i].Rows.Count.ShouldBe( rowCounts[i] );
            }
        }
    }
}
