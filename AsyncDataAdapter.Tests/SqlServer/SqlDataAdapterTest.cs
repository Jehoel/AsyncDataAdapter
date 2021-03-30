using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using NUnit.Framework;

namespace AsyncDataAdapter.Tests.SqlServer
{
    public static class Extensions
    {
        public static void AddParameter( this DbCommand cmd, String name, DbType dbType, Object value )
        {
            DbParameter p = cmd.CreateParameter();

            p.ParameterName = name;
            p.DbType        = dbType;
            p.Value         = value;

            _ = cmd.Parameters.Add( p );
        }
    }

    /// <remarks>Each individual test should take 9-15s to run.</summary>
    public abstract class BaseSqlDataAdapterTest<TDbConnection,TDbCommand,TDbDataAdapter,TAsyncDbAdapter>
        where TDbConnection   : DbConnection
        where TDbCommand      : DbCommand
        where TDbDataAdapter  : DbDataAdapter
        where TAsyncDbAdapter : AsyncDbDataAdapter<TDbCommand>, IAsyncDbDataAdapter, IDisposable
    {
        private const Int32 COMMAND_TIMEOUT = 30; // `SqlCommand.CommandTimeou` is valued in seconds, not milliseconds!

        private static readonly String  _ConnectionString = TestConfiguration.Instance.ConnectionString;
        private static readonly Boolean _Enabled          = TestConfiguration.Instance.DatabaseTestsEnabled;

        #region Utility
        protected abstract TDbConnection CreateConnection( String connectionString );

        protected abstract TDbCommand CreateCommand( TDbConnection connection );

        protected abstract TDbDataAdapter CreateDbAdapter( TDbCommand cmd );

        protected abstract TAsyncDbAdapter CreateAsyncDbAdapter( TDbCommand cmd );

        //

        private async Task<TDbConnection> CreateOpenConnectionAsync( CancellationToken cancellationToken = default )
        {
            if( !_Enabled ) Assert.Inconclusive( message: "Database tests are disabled." );

            TDbConnection conn = this.CreateConnection( _ConnectionString );
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

        private TDbConnection CreateOpenConnection()
        {
            if( !_Enabled ) Assert.Inconclusive( message: "Database tests are disabled." );

            TDbConnection conn = this.CreateConnection( _ConnectionString );
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
            using (TDbConnection conn = await this.CreateOpenConnectionAsync())
            using (TDbCommand cmd = this.CreateCommand(conn))
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
            using (TDbConnection conn = await this.CreateOpenConnectionAsync())
            using (TDbCommand cmd = this.CreateCommand(conn))
            {
                cmd.CommandText = "GetFast";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.AddParameter( "@Number", DbType.Int32, value: 100000 );

                using (TAsyncDbAdapter a = this.CreateAsyncDbAdapter( cmd ))
                {
                    DataTable dt = new DataTable();
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
            using (TDbConnection conn = this.CreateOpenConnection())
            using (TDbCommand cmd = this.CreateCommand(conn))
            {
                cmd.CommandText = "GetFast";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.AddParameter( "@Number", DbType.Int32, value: 100000 );

                using (TDbDataAdapter a = this.CreateDbAdapter( cmd ) )
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
            using (TDbConnection conn = await CreateOpenConnectionAsync())
            using (TDbCommand cmd = this.CreateCommand(conn))
            {
                cmd.CommandText = "GetFast";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.AddParameter( "@Number", DbType.Int32, value: 100000 );

                using (TAsyncDbAdapter a = this.CreateAsyncDbAdapter( cmd ) )
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
            using (TDbConnection conn = this.CreateOpenConnection())
            using (TDbCommand cmd = this.CreateCommand(conn))
            {
                cmd.CommandText = "GetFast";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.AddParameter( "@Number", DbType.Int32, value: 100000 );

                using (TDbDataAdapter a = this.CreateDbAdapter( cmd ))
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
            using (TDbConnection conn = await this.CreateOpenConnectionAsync())
            using (TDbCommand cmd = this.CreateCommand(conn))
            {
                cmd.CommandText = "GetMulti";
                cmd.CommandTimeout = COMMAND_TIMEOUT;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.AddParameter( "@Number1", DbType.Int32, value: 100000 );
                cmd.AddParameter( "@Number2", DbType.Int32, value: 300000 );
                cmd.AddParameter( "@Number3", DbType.Int32, value: 500000 );

                using (TAsyncDbAdapter a = this.CreateAsyncDbAdapter( cmd ) )
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
            using (TDbConnection conn = this.CreateOpenConnection())
            using (TDbCommand cmd = this.CreateCommand(conn))
            {
                cmd.CommandText = "GetMulti";
                cmd.CommandTimeout = COMMAND_TIMEOUT;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.AddParameter( "@Number1", DbType.Int32, value: 100000 );
                cmd.AddParameter( "@Number2", DbType.Int32, value: 300000 );
                cmd.AddParameter( "@Number3", DbType.Int32, value: 500000 );

                using (TDbDataAdapter a = this.CreateDbAdapter(cmd))
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
