using Amazon;
using Amazon.Lambda.S3Events;
using Amazon.Lambda.TestUtilities;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using BAMCIS.LambaFunctions.AWSCURManager.ReportManifest;
using BAMCIS.LambdaFunctions.AWSCURManager;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;
using Xunit;

namespace CURUpdater.Tests
{
    public class FunctionTest
    {
        private static string User = Environment.UserName; // UPDATE THIS VARIABLE
        private static string AccountNumber = "252486826203";
        private static string SourceBucket = $"{User}-billing-delivery";
        private static string DestinationBucket = $"{User}-billing-repo";
        private static string SourceKey = $"{AccountNumber}/GzipDetailedDaily/20181001-20181101/eb3c690f-eeaa-4781-b701-4fd32f8ab19f/GzipDetailedDaily-1.csv.gz";
        private static string SourceManifestKey = $"{AccountNumber}/GzipDetailedDaily/20181001-20181101/GzipDetailedDaily-Manifest.json";
        private static string ProfileName = $"{User}-dev";

        public FunctionTest()
        {
        }

        [Fact]
        public async Task TestOldMethod()
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
            ""key"": ""{SourceKey}"",
            ""size"": 23755,
            ""eTag"": ""076a95085beb214ccd60181be1d9867e"",
            ""sequencer"": ""0A1B2C3D4E5F678901""
          }}
      }}
    }}
  ]
}}
";

            AWSConfigs.AWSProfilesLocation = $"{Environment.GetEnvironmentVariable("UserProfile")}\\.aws\\credentials";
            AWSConfigs.AWSProfileName = ProfileName;
            AmazonS3Config Config = new AmazonS3Config();
            SharedCredentialsFile CredsFile = new SharedCredentialsFile();
            CredsFile.TryGetProfile(ProfileName, out CredentialProfile Profile);

            AWSCredentials Creds = AWSCredentialsFactory.GetAWSCredentials(Profile, CredsFile);

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

            await Entry.Exec2(Event, Context);

            // ASSERT

            // No exception
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

            AWSConfigs.AWSProfilesLocation = $"{Environment.GetEnvironmentVariable("UserProfile")}\\.aws\\credentials";
            AWSConfigs.AWSProfileName = ProfileName;
            AmazonS3Config Config = new AmazonS3Config();
            SharedCredentialsFile CredsFile = new SharedCredentialsFile();
            CredsFile.TryGetProfile(ProfileName, out CredentialProfile Profile);

            AWSCredentials Creds = AWSCredentialsFactory.GetAWSCredentials(Profile, CredsFile);

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

            DateTime DT = JsonConvert.DeserializeObject<DateTime>(Value, new ManifestDateTimeConverter() );

            // ASSERT

            Assert.Equal(new DateTime(2018, 10, 01, 0, 0, 0, DateTimeKind.Utc), DT);
            Assert.Equal(Value, JsonConvert.SerializeObject(DT, new ManifestDateTimeConverter()));

        }
    }
}
