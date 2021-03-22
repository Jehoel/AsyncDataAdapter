using System;
using System.IO;

using Microsoft.Extensions.Configuration;

namespace AsyncDataAdapter
{
    public class TestConfiguration
    {
//      private const string _defaultConnectionString = @"server=.\sqlexpress;database=AsyncDataReaderTest;Trusted_Connection=Yes";
        private const string _defaultConnectionString = @"server=.\SQL2017;database=AsyncDataReaderTest;Trusted_Connection=Yes";

        public static TestConfiguration Instance { get; } = new TestConfiguration();

        /// <summary>See https://stackoverflow.com/questions/39791634/read-appsettings-json-values-in-net-core-test-project/47591692</summary>
        private TestConfiguration()
        {
            const string fileName = "test-config.json";
            if( File.Exists( fileName ))
            {
                IConfigurationRoot config = new ConfigurationBuilder()
                    .AddJsonFile( fileName )
                    .Build();

                this.ConnectionString = config["ConnectionString"];
            }

            if( string.IsNullOrWhiteSpace(this.ConnectionString) )
            {
                this.ConnectionString = _defaultConnectionString;
            }
        }

        public String ConnectionString { get; }

        /* Sample appconfig json (note the '\' is escaped!):

        {
          "ConnectionString": "server=.\\SQL2017;database=AsyncDataReaderTest;Trusted_Connection=Yes"
        }

        */
    }
}
