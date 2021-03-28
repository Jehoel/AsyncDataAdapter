using System.Collections.Generic;

using AsyncDataAdapter.Tests.FakeDb;

using NUnit.Framework;

using Shouldly;

namespace AsyncDataAdapter.Tests.MetaTests
{
    /// <summary>Just some meta-tests so I know my <see cref="FakeDbCommandBuilder"/> class works.</summary>
    public class FakeDbCommandBuilderTests
    {
        [Test]
        public void FakeDbCommandBuilder_should_work()
        {
            List<TestTable> randomDataSource = RandomDataGenerator.CreateRandomTables( seed: 1234, tableCount: 5, /*allowZeroRowsInTablesByIdx: */ 1, 3 );

            using( FakeDbConnection connection = new FakeDbConnection( asyncMode: AsyncMode.AllowSync ) )
            using( FakeDbCommand selectCommand = connection.CreateCommand( testTables: randomDataSource ) )
            {
                connection.Open();

                using( FakeDbDataAdapter adpt = new FakeDbDataAdapter( selectCommand ) )
                using( FakeDbCommandBuilder cmdBuilder = adpt.CreateCommandBuilder() )
                {
                    cmdBuilder.DataAdapter.ShouldBe( adpt );
                    
                    FakeDbCommand deleteCommand1 = cmdBuilder.GetDeleteCommand(); // Same as ` useColumnsForParameterNames: false );`
                    FakeDbCommand updateCommand1 = cmdBuilder.GetUpdateCommand();
                    FakeDbCommand insertCommand1 = cmdBuilder.GetInsertCommand();

                    FakeDbCommand deleteCommand2 = cmdBuilder.GetDeleteCommand( useColumnsForParameterNames: true );
                    FakeDbCommand updateCommand2 = cmdBuilder.GetUpdateCommand( useColumnsForParameterNames: true );
                    FakeDbCommand insertCommand2 = cmdBuilder.GetInsertCommand( useColumnsForParameterNames: true );

                    _ = deleteCommand1.ShouldNotBeNull();
                    _ = updateCommand1.ShouldNotBeNull();
                    _ = insertCommand1.ShouldNotBeNull();

                    _ = deleteCommand2.ShouldNotBeNull();
                    _ = updateCommand2.ShouldNotBeNull();
                    _ = insertCommand2.ShouldNotBeNull();
                }
            }
        }
    }
}
