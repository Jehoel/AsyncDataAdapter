using System;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using NUnit.Framework;

namespace AsyncDataAdapter.Tests
{
    /// <remarks>Each individual test should take 9-15s to run.</summary>
    [TestFixture]
    public class SqlDataAdapterTest
    {
        private const Int32 COMMAND_TIMEOUT = 30; // `SqlCommand.CommandTimeou` is valued in seconds, not milliseconds!

        private static readonly String  _ConnectionString = TestConfiguration.Instance.ConnectionString;
        private static readonly Boolean _Enabled          = TestConfiguration.Instance.DatabaseTestsEnabled;

        #region Utility
        private static async Task<SqlConnection> CreateOpenConnectionAsync( CancellationToken cancellationToken = default )
        {
            if( !_Enabled ) Assert.Inconclusive( message: "Database tests are disabled." );

            SqlConnection conn = new SqlConnection(_ConnectionString);
            try
            {
                await conn.OpenAsync(cancellationToken);
                return conn;
            }
            catch
            {
                conn.Dispose();
                throw;
            }
        }

        private static SqlConnection CreateOpenConnection()
        {
            if( !_Enabled ) Assert.Inconclusive( message: "Database tests are disabled." );

            SqlConnection conn = new SqlConnection(_ConnectionString);
            try
            {
                conn.Open();
                return conn;
            }
            catch
            {
                conn.Dispose();
                throw;
            }
        }

        [OneTimeSetUp]
        public async Task Setup()
        {
            using (SqlConnection conn = await CreateOpenConnectionAsync())
            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = "dbo.ResetTab1";
                cmd.CommandType = CommandType.StoredProcedure;
                
                _ = await cmd.ExecuteNonQueryAsync();
            }
        }

        #endregion

        #region Fill(DataTable)

        [Test]
        public async Task FillAsyncDataTable()
        {
            using (SqlConnection conn = await CreateOpenConnectionAsync())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "GetFast";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

                using (MSqlAsyncDbDataAdapter a = new MSqlAsyncDbDataAdapter( cmd ))
                {
                    var dt = new DataTable();
                    var r = await a.FillAsync(dt);

                    Assert.AreEqual(900000, r);
                    Assert.AreEqual(900000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        [Test]
        public void FillDataTable()
        {
            using (SqlConnection conn = CreateOpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "GetFast";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

                using (SqlDataAdapter a = new SqlDataAdapter(cmd))
                {
                    var dt = new DataTable();
                    var r = a.Fill(dt);

                    Assert.AreEqual(900000, r);
                    Assert.AreEqual(900000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        #endregion

        #region Fill(DataSet)

        [Test]
        public async Task FillAsyncDataSet()
        {
            using (SqlConnection conn = await CreateOpenConnectionAsync())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "GetFast";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

                using (MSqlAsyncDbDataAdapter a = new MSqlAsyncDbDataAdapter( cmd ) )
                {
                    var ds = new DataSet();
                    var r = await a.FillAsync(ds);

                    Assert.AreEqual(1, ds.Tables.Count);
                    var dt = ds.Tables[0];

                    Assert.AreEqual(900000, r);
                    Assert.AreEqual(900000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        [Test]
        public void FillDataSet()
        {
            using (SqlConnection conn = CreateOpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "GetFast";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

                using (SqlDataAdapter a = new SqlDataAdapter( cmd))
                {
                    var ds = new DataSet();
                    var r = a.Fill(ds);

                    Assert.AreEqual(1, ds.Tables.Count);

                    var dt = ds.Tables[0];

                    Assert.AreEqual(900000, r);
                    Assert.AreEqual(900000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        #endregion

        #region Fill(DataSet) - dbo.GetMulti

        [Test]
        public async Task FillAsyncDataSetMulti()
        {
            using (SqlConnection conn = await CreateOpenConnectionAsync())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "GetMulti";
                cmd.CommandTimeout = COMMAND_TIMEOUT;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@Number1", SqlDbType.Int).Value = 100000;
                cmd.Parameters.Add("@Number2", SqlDbType.Int).Value = 300000;
                cmd.Parameters.Add("@Number3", SqlDbType.Int).Value = 500000;

                using (MSqlAsyncDbDataAdapter a = new MSqlAsyncDbDataAdapter( cmd))
                {
                    var ds = new DataSet();

                    Stopwatch sw = Stopwatch.StartNew();
                    var rowsRead = await a.FillAsync(ds);
                    sw.Stop();
                    Assert.GreaterOrEqual(sw.Elapsed.TotalSeconds, 7); // There are 7 `WAITFOR DELAY '00:00:01'` statements in the procedure.
                    Assert.AreEqual(8, ds.Tables.Count);

                    var dt = ds.Tables[0];

                    Assert.AreEqual(50000, rowsRead);
                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);

                    dt = ds.Tables[6];

                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);

                    dt = ds.Tables[7];

                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        [Test]
        public void FillDataSetMulti()
        {
            using (SqlConnection conn = CreateOpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "GetMulti";
                cmd.CommandTimeout = COMMAND_TIMEOUT;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@Number1", SqlDbType.Int).Value = 100000;
                cmd.Parameters.Add("@Number2", SqlDbType.Int).Value = 300000;
                cmd.Parameters.Add("@Number3", SqlDbType.Int).Value = 500000;

                using (SqlDataAdapter a = new SqlDataAdapter(cmd))
                {
                    var ds = new DataSet();

                    Stopwatch sw = Stopwatch.StartNew();
                    var rowsRead = a.Fill(ds);
                    sw.Stop();
                    Assert.GreaterOrEqual(sw.Elapsed.TotalSeconds, 7); // There are 7 `WAITFOR DELAY '00:00:01'` statements in the procedure.
                    Assert.AreEqual(8, ds.Tables.Count);

                    var dt = ds.Tables[0];

                    Assert.AreEqual(50000, rowsRead);
                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);

                    dt = ds.Tables[6];

                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);

                    dt = ds.Tables[7];

                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        private static void AssertDataTableContent(DataTable dt)
        {
            int i = 1;

            do
            {
                var previousRow = dt.Rows[i - 1];

                var flt = (double) previousRow["FltVal"];
                var dec = (decimal) previousRow["DecVal"];
                var st = (DateTime) previousRow["StartDate"];
                var txt = (string) previousRow["Txt"];

                flt += .1f;
                dec += (decimal) .1;

                var currentRow = dt.Rows[i];

                var aflt = (double) currentRow["FltVal"];
                var adec = (decimal) currentRow["DecVal"];
                var ast = (DateTime) currentRow["StartDate"];
                var atxt = (string) currentRow["Txt"];

                Assert.AreEqual(flt, aflt, .01);
                Assert.AreEqual(dec, adec);
                Assert.AreEqual(st, ast);
                Assert.AreEqual(txt, atxt);
                i++;
            }
            while (i < dt.Rows.Count);
        }

        #endregion
    }
}
