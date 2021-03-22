# AsyncDataAdapter

Implementation of asynchronous methods on SqlDataAdapter (support for async/await).

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

* Branches:
  * The `DotNetSource` branch contains the original Microsoft .NET sources.
    * There was originally an intention to keep this project's `AsyncDataReader` in-sync with the upstream .NET Framework and .NET Core `System.Data.DataReader` and `System.Data.Common.DbDataReader`, but I decided that was probably a waste of time. The orignal source is kept-around for reference.

* Future development goals:
  * I'd like to move-away from `AdaDataAdapter` and `AdaDbDataAdapter` being completely new implementations.
  * I did have an earlier version I wrote myself (not published) where the original `System.Data.Common.DbDataAdapter` was used as the base-class.
    * This way the new async functionality was purely additive, and the synchronous API was still available.
  * I also want to get Appveyor working with this fork.

* Advisory: this project will likely be short-lived because Microsoft will eventually implement async support in their `DbDataAdapter` and subclasses, likely before .NET 7 around 2023.

## Nuget package

#### Version 3.0+

[https://www.nuget.org/packages/Jehoel.AsyncDataAdapter/](https://www.nuget.org/packages/Jehoel.AsyncDataAdapter/)

#### Version 1.0 - 2.0

[https://www.nuget.org/packages/AsyncDataAdapter/](https://www.nuget.org/packages/AsyncDataAdapter/)

## Usage

* `IDataAdapter2`
  * This interface extends `IDataAdapter` with `Task<Int32> FillAsync( DataSet, CancellationToken )`.
* `IDataAdapter3`
  * This interface extends `IDataAdapter2` with `Task<Int32> UpdateAsync( DataSet, CancellationToken )`.
* `AsyncDataAdapter.AdaDataAdapter`
  * `public abstract class AdaDataAdapter : IDataAdapter3`
  * This is a reimplementation of `System.Data.DataAdapter` that currently _only_ supports Async operations.
    * i.e. do not use this type as a drop-in replacement for existing code.
  * The non-async (synchronous, blocking) methods `Fill`, `FillSchema`, and `Update` are not yet implemented and will throw `NotImplementedException`.
* `AsyncDataAdapter.AdaDbDataAdapter`
  * `public abstract class AdaDbDataAdapter : AdaDataAdapter`
  * This is a reimplementation of `System.Data.DbDataAdapter` that currently _only_ supports Async operations.
    * i.e. do not use this type as a drop-in replacement for existing code.
  * The non-async (synchronous, blocking) methods `Fill`, `FillSchema`, and `Update` are not yet implemented and will throw `NotImplementedException`.
  * The difference between `AdaDataAdapter` and `AdaDbDataAdapter` is like the difference between `DataAdapter` and `DbDataAdapter`:
    * ...which is to say it's _complicated_. But in short (and far as I know)...
      * the original design of ADO.NET had `DataAdapter` to support the broadest kinds of data-sources, including non-relational-database sources, such as in-memory object collections.
      * while `DbDataAdapter` derives from `DataAdapter` to add common functionality needed to support two-way data movement (with `Fill` to retrieve data, and `Update` to push data) so that RDMBS-specific implementations like `SqlDataAdapter` and `OracleDataAdapter` don't need to reimplement the entire functionality of a `DataAdapter`.
      * Of course, the past 20+ years of .NET development experience shows that almost everyone subclasses `DbDataAdapter` even when it isn't necessary.

* `AdaSqlDataAdapter`
  * This is the only concrete (non-`abstract`) `DataReader` type in the library - so generally speaking, this is the only type you need to concern yourself with.

* There is no `AsyncOleDbDataAdapter` or `AsyncOdbcDataAdapter` because the lower-level OLE-DB and ODBC do not expose an async-friendly interface.
  * Indeed, if you look at what happens when you use `OleDbCommand.ExecuteReaderAsync` or `OdbcCommand.ExecuteNonQueryAsync` they're just thin-wrappers over _fake async_ methods (that either block the thread, or run the method in the background in the thread pool).#
    * Because of this, there is no async support for OLE-DB and ODBC. Pull-requests that implement an async DataReader with _fake async_ will not be accepted.

### Sample usage for FillAsync with DataTable and CancellationTokenSource

```csharp
using (CancellationTokenSource cts = new CancellationTokenSource( TimeSpan.FromSeconds(15) ))
using (SqlConnection conn = new SqlConnection( this.connectionString ))
{
    await conn.OpenAsync( cts.Token );

    using (SqlCommand c = conn.CreateCommand())
    {
        c.CommandText = "GetFast";
        c.CommandType = CommandType.StoredProcedure;
        c.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

        using(AdaSqlDataAdapter a = new AdaSqlDataAdapter(c))
        {
            DataTable singleTable = new DataTable();
            Int32 rows = await a.FillAsync(singleTable, cts.Token );
            return ds;
        }
    }
}
```

### Sample usage for DataSet

```csharp
using (SqlConnection conn = new SqlConnection( this.connectionString ))
{
    await conn.OpenAsync();

    using (SqlCommand c = conn.CreateCommand())
    {
        c.CommandText = "GetFast";
        c.CommandType = CommandType.StoredProcedure;
        c.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

        using(AdaSqlDataAdapter a = new AdaSqlDataAdapter(c))
        {
            DataSet ds = new DataSet();
            Int32 rows = await a.FillAsync(ds);
            return ds;
        }
    }
}
```
