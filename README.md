# AsyncDataAdapter

Implementation of asynchronous methods for ADO.NET

The implementation is based on source code provided by Microsoft on GitHub.

## Onboarding and contributing

<!-- TODO: Set-up Appveyor for this...
[![Build status](https://ci.appveyor.com/api/projects/status/bw8gl0fp62vmia15/branch/master?svg=true)](https://ci.appveyor.com/project/voloda/asyncdataadapter/branch/master)
-->

* Tooling: Visual Studio 2019 with .NET Core workload
* Dependencies:
  * .NET Standard 2.0 for the `AsyncDataAdapter` library.
  * .NET Core 3.1 for the `AsyncDataAdapter.Test` project.
    * Currently uses NUnit 3.x, but I'd like to move-over to xUnit.

* Future development goals:
  * I also want to get Appveyor working with this fork.
  * The `Update` and `UpdateAsync` methods need working test-cases.
  * Internally the project uses reflection to workaround some rather unfortunate design-decisions in the original `DbDataAdapter` and `DbCommandBuilder` classes - so these features may fail in some future update to .NET. Also they use `MethodInfo.Invoke` which can be improved by using Dynamic-Methods (i.e. IL-generation).
    * It would be a good idea to replace direct calls to reflected methods to interfaces that allow consumers to provide their own methods, e.g. to work-around any breaking changes to reflection in future builds of .NET, and so on.

* Advisory: this project will likely be short-lived because Microsoft will eventually implement async support in their `DbDataAdapter` and subclasses, likely before .NET 7 around 2023.

## Nuget package

#### Version 4.0

* [https://www.nuget.org/packages/Jehoel.AsyncDataAdapter/](https://www.nuget.org/packages/Jehoel.AsyncDataAdapter/)
* [https://www.nuget.org/packages/Jehoel.AsyncDataAdapter.System.Data.SqlClient/](https://www.nuget.org/packages/Jehoel.AsyncDataAdapter.System.Data.SqlClient/)
* [https://www.nuget.org/packages/Jehoel.AsyncDataAdapter.Microsoft.Data.SqlClient/](https://www.nuget.org/packages/Jehoel.AsyncDataAdapter.Microsoft.Data.SqlClient/)

#### Version 3.0

[https://www.nuget.org/packages/Jehoel.AsyncDataAdapter/](https://www.nuget.org/packages/Jehoel.AsyncDataAdapter/)

#### Version 1.0 - 2.0

[https://www.nuget.org/packages/AsyncDataAdapter/](https://www.nuget.org/packages/AsyncDataAdapter/)

## Usage

You can use the asynchronous methods using either the provider-specific concrete subclasses (such as `SqlAsyncDbDataAdapter`, for `System.Data.SqlClient`; and `MSSqlAsyncDbDataAdapter` for `Microsoft.Data.SqlClient`) - or via the interfaces `IAsyncDataAdapter`, `IAsyncDbDataAdapter`, `IUpdatingAsyncDataAdapter`, and `IUpdatingAsyncDbDataAdapter`.

### Provider-specific subclasses

* `SqlAsyncDbDataAdapter`
  * You will need to add a dependency to the `Jehoel.AsyncDataAdapter.System.Data.SqlClient` NuGet package.
* `MSSqlAsyncDbDataAdapter`
  * You will need to add a dependency to the `Jehoel.AsyncDataAdapter.Microsoft.Data.SqlClient` NuGet package.
* There is no `AsyncDbDataAdapter` class for `System.Data.OleDb` nor `System.Data.Odbc` because the lower-level OLE-DB and ODBC APIs do not expose an async-capable interface (at least, as far as I know).
  * While the base class `System.Data.Common.DbDataReader` does have a `RaedAsync` method, it's a _fake-async_ event: Indeed, if you look at what happens when you use `OleDbCommand.ExecuteReaderAsync` or `OdbcCommand.ExecuteNonQueryAsync` they're just thin-wrappers over _fake async_ methods (that either block the thread, or run the method in the background in the thread pool).#
    * Because of this, there is no async support for OLE-DB and ODBC in this library. Pull-requests that implement an async DataReader with _fake async_ will not be accepted.

### Interfaces

* `IAsyncDataAdapter` and `IAsyncDbDataAdapter`
* These interfaces extend `IDataAdapter` and `IDbDataAdapter` respectively with these new methods:
    * `Task<Int32> FillAsync( DataSet, CancellationToken )`
    * `Task<DataTable[]> FillSchemaAsync( DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken )`.
* `IUpdatingAsyncDataAdapter` and `IUpdatingAsyncDbDataAdapter`
* These interfaces extend `IAsyncDataAdapter` and `IAsyncDbDataAdapter` respectively with this method:
    * `Task<Int32> UpdateAsync( DataSet dataSet, CancellationToken cancellationToken );`

### Sample usage for `FillAsync(DataTable)` and CancellationTokenSource

```csharp
using (CancellationTokenSource cts = new CancellationTokenSource( TimeSpan.FromSeconds(15) ))
using (SqlConnection conn = new SqlConnection( this.connectionString ))
{
    await conn.OpenAsync( cts.Token );

    using (SqlCommand cmd = conn.CreateCommand())
    {
        cmd.CommandText = "dbo.GetFast";
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

        using(SqlAsyncDbDataAdapter adapter = cmd.CreateAsyncAdapter()) // `CreateAsyncAdapter` is an extension method.
        {
            DataTable singleTable = new DataTable();
            Int32 rows = await a.FillAsync(singleTable, cts.Token );
            return ds;
        }
    }
}
```

### Sample usage for `FillAsync(DataSet)`, without any CancellationToken

```csharp
using (SqlConnection conn = new SqlConnection( this.connectionString ))
{
    await conn.OpenAsync();

    using (SqlCommand c = conn.CreateCommand())
    {
        c.CommandText = "GetFast";
        c.CommandType = CommandType.StoredProcedure;
        c.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

        using(SqlAsyncDbDataAdapter adapter = cmd.CreateAsyncAdapter())
        {
            DataSet ds = new DataSet();
            Int32 rows = await a.FillAsync(ds);
            return ds;
        }
    }
}

### Sample usage for `UpdateAsync(DataSet)`, without any CancellationToken

```csharp
using (SqlConnection conn = new SqlConnection( this.connectionString ))
using (SqlCommand selectCmd = conn.CreateCommand())
{
    c.CommandText = "GetFast";
    c.CommandType = CommandType.StoredProcedure;
    c.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

    using(MSSqlAsyncDbDataAdapter adapter = cmd.CreateAsyncAdapter())
    using(IAsyncDbCommandBuilder cmdBuilder = await adapter.CreateCommandBuilderAsync().ConfigureAwait(false) )
    {
        DataSet dataSet = new DataSet();
        _ = await a.FillAsync( dataSet );
        
        adapter.UpdateCommand = cmdBuilder.GetUpdateCommand();
        
        _ = await adapter.UpdateAsync( dataSet );
    }
}
}
```

## Regarding `Update` and `UpdateAsync`

* This new library does support `DbDataAdapter.Update` and `UpdateAsync` with test coverage for all overloads.
  * This library also includes an async `DbCommandBuilder` base class.
    * This `DbCommandBuilder` class is necessary because the current base `DbCommandBuilder` in ADO.NET makes synchronous database calls to populate its internal schema cache.
* The `UpdateAsync` methods should all work _in principle_ however I am not currently able to test them adequately.
  * Indeed, the tests for `Update` and `UpdateAsync` currently fail because I honestly have no idea how to thoroughly and correctly test how `Update` and `UpdateAsync` work with respect to different Table Mappings and multi-table (`DataSet` and `DataTable[]`) updates.
  * There is a dearth of documentation available about this functionality, which is unfortunate, but understandable, given that Entity Framework has fully replaced the need for `DbDataAdapter` in my opinion.
* If you're looking for `UpdateAsync` support, then please see
