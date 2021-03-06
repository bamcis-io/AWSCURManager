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
        private static string AccountNumber = Environment.GetEnvironmentVariable("ACCOUNT_ID"); // UPDATE THIS TO YOUR ACCOUNT NUMBER
        private static string Region = Environment.GetEnvironmentVariable("REGION"); // UPDATE THIS TO YOUR REGION
        private static string AWSPartition = Environment.GetEnvironmentVariable("PARTITION"); // UPDATE THIS TO YOUR REGION
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

        static FunctionTest()
        {
            AWSConfigs.AWSProfilesLocation = $"{Environment.GetEnvironmentVariable("UserProfile")}\\.aws\\credentials";
            AWSConfigs.AWSProfileName = ProfileName;
            if (CredsFile.TryGetProfile(ProfileName, out CredentialProfile Profile))
            {
                Creds = AWSCredentialsFactory.GetAWSCredentials(Profile, CredsFile);
            }
        }

        public FunctionTest()
        {
        }

        private async Task TestManifestFile()
        {
            // ARRANGE
            string Json = $@"
{{
    ""Records"": [
      {{
        ""eventVersion"": ""2.0"",
        ""eventSource"": ""aws:s3"",
        ""awsRegion"": ""{Region}"",
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
            ""arn"": ""arn:{AWSPartition}:s3:::{SourceBucket}""
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
                InvokedFunctionArn = $"arn:{AWSPartition}:lambda:{Region}:{AccountNumber}:function:CURManager"
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

        private async Task TestNonExistentGlueTable()
        {
            // ARRANGE

            IAmazonGlue GlueClient;
            if (Creds != null)
            {
                GlueClient = new AmazonGlueClient(Creds);
            }
            else
            {
                GlueClient = new AmazonGlueClient();
            }

            GetTableRequest Request = new GetTableRequest()
            {
                DatabaseName = "test",
                Name = "test"
            };

            // ACT / ASSERT
            await Assert.ThrowsAsync<EntityNotFoundException>(async () => await GlueClient.GetTableAsync(Request));
        }

        private async Task TestLaunchJob()
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

            IAmazonGlue GlueClient;
            if (Creds != null)
            {
                GlueClient = new AmazonGlueClient(Creds);
            }
            else
            {
                GlueClient = new AmazonGlueClient();
            }

            // ACT
            StartJobRunResponse Response = await GlueClient.StartJobRunAsync(Request);

            // ASSERT

            Assert.NotNull(Response);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);           
        }
    }
}
