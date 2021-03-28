using System.Data;
using System.Data.Common;

using NUnit.Framework;

using AsyncDataAdapter.Internal;

namespace AsyncDataAdapter.Tests
{
    [TestFixture]
    public class ReflectionTest
    {
        [Test]
        public void AdapterInitIntShouldWork()
        {
            var e = new RowUpdatedEventArgs(null, null, StatementType.Select, null);

            Assert.DoesNotThrow(() => e.AdapterInit_( rowCount: 10 ) );
        }

        [Test]
        public void AdapterInitDataRowArrayShouldWork()
        {
            var e = new RowUpdatedEventArgs(null, null, StatementType.Select, null);

            Assert.DoesNotThrow(() => e.AdapterInit_( rowBatch: new DataRow[0]));
        }

        [Test]
        public void GetRowsShouldWork()
        {
            var t = new DataTable();

            var dataRow = t.NewRow();

            var e = new RowUpdatedEventArgs(null, null, StatementType.Select, null);
            e.AdapterInit_(new []{dataRow});

            var rows = e.GetRows_();

            Assert.AreEqual(1, rows.Length);
            Assert.AreEqual(dataRow, rows[0]);
        }

        [Test]
        public void GetRowsForSingleRowShouldWork()
        {
            var t = new DataTable();

            var dataRow = t.NewRow();

            var e = new RowUpdatedEventArgs(null, null, StatementType.Select, null);
            e.AdapterInit_(new []{dataRow});

            var row = e.GetRow_(0);

            Assert.AreEqual(dataRow, row);
        }

        [Test]
        public void EnsureAdditionalCapacityShouldWork()
        {
            var c = new DataTable().Columns;
            
            Assert.DoesNotThrow(() => c.EnsureAdditionalCapacity_(10));
        }

        
    }
}
