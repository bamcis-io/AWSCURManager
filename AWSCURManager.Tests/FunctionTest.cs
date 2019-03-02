using Amazon;
using Amazon.Glue;
using Amazon.Glue.Model;
using Amazon.Lambda.S3Events;
using Amazon.Lambda.TestUtilities;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using BAMCIS.LambaFunctions.AWSCURManager.ReportManifest;
using BAMCIS.LambdaFunctions.AWSCURManager;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;

namespace CURUpdater.Tests
{
    public class FunctionTest
    {
        private static string User = Environment.UserName; // UPDATE THIS VARIABLE
        private static string AccountNumber = Environment.GetEnvironmentVariable("AWSDevAccountId"); // UPDATE THIS TO YOUR ACCOUNT NUMBER
        private static string SourceBucket = $"{User}-billing-delivery"; // UPDATE THIS TO MATCH YOUR SOURCE BUCKET
        private static string DestinationBucket = $"{User}-billing-repo"; // UPDATE THIS TO MATCH YOUR DESTINATION BUCKET
        private static string SourceKey = $"{AccountNumber}/GzipDetailedDaily/20181001-20181101/eb3c690f-eeaa-4781-b701-4fd32f8ab19f/GzipDetailedDaily-1.csv.gz";
        private static string ParquetKey = $"{AccountNumber}/TestReport/TestReport/year=2019/month=2/TestReport-00001.snappy.parquet";
        private static string SourceManifestKey = $"{AccountNumber}/GzipDetailedDaily/20181001-20181101/GzipDetailedDaily-Manifest.json";
        private static string ProfileName = $"{User}-dev";
        private static SharedCredentialsFile CredsFile = new SharedCredentialsFile();
        private static AWSCredentials Creds;

        private static string _YearMonthDay = "(20[0-9]{2})((?:0[1-9]|1[0-2]))((?:0[1-9]|[1-2][0-9]|3[0-1]))";
        private static string _Guid = "{?[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[0-9a-f]{4}-?[0-9a-f]{12}}?";
        private static Regex _DateAndGuidRegex = new Regex($"{_YearMonthDay}-{_YearMonthDay}\\/({_Guid})", RegexOptions.IgnoreCase);
        private static Regex _CsvExtensionRegex = new Regex(@"(\.csv(?:\.gz|\.zip)?)$");
        private static Regex _ParquetExtensionRegex = new Regex(@"((?:\.snappy)?\.parquet)$");
        private static Regex _ParquetPathRegex = new Regex(@"year=(20[0-9]{2})\/month=((?:0?[1-9]|1[0-2]))", RegexOptions.IgnoreCase);

        static FunctionTest()
        {
            AWSConfigs.AWSProfilesLocation = $"{Environment.GetEnvironmentVariable("UserProfile")}\\.aws\\credentials";
            AWSConfigs.AWSProfileName = ProfileName;
            CredsFile.TryGetProfile(ProfileName, out CredentialProfile Profile);
            Creds = AWSCredentialsFactory.GetAWSCredentials(Profile, CredsFile);
        }

        public FunctionTest()
        {
        }

        [Fact]
        public async Task TestManifestFile()
        {
            // ARRANGE
            string Json = $@"
{{
    ""Records"": [
      {{
        ""eventVersion"": ""2.0"",
        ""eventSource"": ""aws:s3"",
        ""awsRegion"": ""us-east-1"",
        ""eventTime"": ""2018-10-01T01:00:00.000Z"",
        ""eventName"": ""ObjectCreated:Put"",
        ""userIdentity"": {{
          ""principalId"": ""EXAMPLE""
        }},
        ""requestParameters"": {{
          ""sourceIPAddress"": ""127.0.0.1""
        }},
        ""responseElements"": {{
          ""x-amz-request-id"": ""EXAMPLE123456789"",
          ""x-amz-id-2"": ""EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH""
        }},
        ""s3"": {{
          ""s3SchemaVersion"": ""1.0"",
          ""configurationId"": ""testConfigRule"",
          ""bucket"": {{
            ""name"": ""{SourceBucket}"",
            ""ownerIdentity"": {{
              ""principalId"": ""EXAMPLE""
            }},
            ""arn"": ""arn:aws:s3:::{SourceBucket}""
          }},
          ""object"": {{
            ""key"": ""{SourceManifestKey}"",
            ""size"": 7658,
            ""eTag"": ""0409fb62239b5d5daa27a2a1982c4dc2"",
            ""sequencer"": ""0A1B2C3D4E5F678901""
          }}
      }}
    }}
  ]
}}
";
            TestLambdaLogger TestLogger = new TestLambdaLogger();
            TestClientContext ClientContext = new TestClientContext();

            TestLambdaContext Context = new TestLambdaContext()
            {
                FunctionName = "CURManager",
                FunctionVersion = "1",
                Logger = TestLogger,
                ClientContext = ClientContext,
                LogGroupName = "aws/lambda/CURManager",
                LogStreamName = Guid.NewGuid().ToString(),
                RemainingTime = TimeSpan.FromSeconds(300),
                InvokedFunctionArn = $"arn:aws:lambda:us-east-1:{AccountNumber}:function:CURManager"
            };

            S3Event Event = JsonConvert.DeserializeObject<S3Event>(Json);

            Environment.SetEnvironmentVariable("DESTINATION_S3_BUCKET", DestinationBucket);

            Entrypoint Entry = new Entrypoint();

            // ACT

            await Entry.Exec(Event, Context);

            // ASSERT

            // No exception
        }

        [Fact]
        public void TestBillingPeriodDeserialization()
        {
            // ARRANGE
            string Value = "\"20181001T000000.000Z\"";

            // ACT

            DateTime DT = JsonConvert.DeserializeObject<DateTime>(Value, new ManifestDateTimeConverter());

            // ASSERT

            Assert.Equal(new DateTime(2018, 10, 01, 0, 0, 0, DateTimeKind.Utc), DT);
            Assert.Equal(Value, JsonConvert.SerializeObject(DT, new ManifestDateTimeConverter()));

        }

        [Fact]
        public async Task TestNonExistentGlueTable()
        {
            // ARRANGE
            IAmazonGlue GlueClient = new AmazonGlueClient(Creds);

            GetTableRequest Request = new GetTableRequest()
            {
                DatabaseName = "test",
                Name = "test"
            };

            // ACT / ASSERT
            await Assert.ThrowsAsync<EntityNotFoundException>(async () => await GlueClient.GetTableAsync(Request));
        }

        [Fact]
        public async Task TestLaunchJob()
        {
            //ARRANGE

            StartJobRunRequest Request = new StartJobRunRequest()
            {
                JobName = "CUR File ETL",
                Timeout = 1440, // 24 Hours          
                Arguments = new Dictionary<string, string>()
                        {
                            { "--table", "2018-10-01" },
                            { "--database", "billingdata" },
                            { "--destination_bucket", $"{User}-billing-formatted" }
                        }
            };

            IAmazonGlue GlueClient = new AmazonGlueClient(Creds);

            // ACT
            StartJobRunResponse Response = await GlueClient.StartJobRunAsync(Request);

            // ASSERT

            Assert.NotNull(Response);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);           
        }

        [Fact]
        public void CsvKeyRegexTest()
        {
            // ARRANGE

            // ACT 
            Match Result = _DateAndGuidRegex.Match(SourceKey);

            // ASSERT
            Assert.True(Result.Success);
            Assert.Equal("2018", Result.Groups[1].Value);
            Assert.Equal("10", Result.Groups[2].Value);
            Assert.Equal("01", Result.Groups[3].Value);
            Assert.Equal("2018", Result.Groups[4].Value);
            Assert.Equal("11", Result.Groups[5].Value);
            Assert.Equal("01", Result.Groups[6].Value);
        }

        [Fact]
        public void ParquetKeyRegexTest()
        {
            // ARRANGE

            // ACT
            Match Result = _ParquetPathRegex.Match(ParquetKey);

            // ASSERT
            Assert.True(Result.Success);
            Assert.Equal("2019", Result.Groups[1].Value);
            Assert.Equal("2", Result.Groups[2].Value);
        }
    }
}
