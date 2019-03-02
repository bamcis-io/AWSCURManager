using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Glue;
using Amazon.Glue.Model;
using BAMCIS.AWSLambda.Common;
using BAMCIS.LambaFunctions.AWSCURManager.ReportManifest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static Amazon.S3.Util.S3EventNotification;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using System.Threading;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BAMCIS.LambdaFunctions.AWSCURManager
{
    /// <summary>
    /// Entrypoint class for the Lambda function
    /// </summary>
    public class Entrypoint
    {
        #region Private Fields

        private static IAmazonS3 _S3Client;
        private static IAmazonGlue _GlueClient;
        private static IAmazonSimpleNotificationService _SNSClient;
        private static string _SNSTopic;
        private static readonly string _Subject = "CUR Manager Failure";
        private static string _YearMonthDay = "(20[0-9]{2})((?:0[1-9]|1[0-2]))((?:0[1-9]|[1-2][0-9]|3[0-1]))";
        private static string _Guid = "{?[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[0-9a-f]{4}-?[0-9a-f]{12}}?";

        // Groups:
        // 1 = Year
        // 2 = Month
        // 3 = Day
        // 4 = Year
        // 5 = Month
        // 6 = Day
        // 7 = Guid
        private static Regex _DateAndGuidRegex = new Regex($"{_YearMonthDay}-{_YearMonthDay}/({_Guid})", RegexOptions.IgnoreCase);

        private static string _DestinationBucket;
        private static string _GlueJobName;
        private static string _GlueDatabaseName;
        private static string _GlueDestinationBucket;
        private static string _OutputPathFormat;

        #endregion

        #region Constructors

        /// <summary>
        /// Static constructor
        /// </summary>
        static Entrypoint()
        {
            _S3Client = new AmazonS3Client();
            _GlueClient = new AmazonGlueClient();
            _SNSClient = new AmazonSimpleNotificationServiceClient();
            _DestinationBucket = Environment.GetEnvironmentVariable("DESTINATION_S3_BUCKET");
            _GlueJobName = Environment.GetEnvironmentVariable("GLUE_JOB_NAME");
            _SNSTopic = Environment.GetEnvironmentVariable("SNS_TOPIC");
            _GlueDatabaseName = Environment.GetEnvironmentVariable("DATABASE_NAME");
            _GlueDestinationBucket = Environment.GetEnvironmentVariable("GLUE_DESTINATION_BUCKET");
            _OutputPathFormat = Environment.GetEnvironmentVariable("OUTPUT_PATH_FORMAT");

            // Use ? in case output path format is null
            _OutputPathFormat = _OutputPathFormat?.ToLower();

            if (_OutputPathFormat != "billingperiod" && _OutputPathFormat != "yearmonth")
            {
                _OutputPathFormat = "billingperiod";
            }
        }

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Entrypoint()
        {
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Entrypoint for the lambda function, processes each manifest file
        /// </summary>
        /// <param name="s3Event"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task Exec(S3Event s3Event, ILambdaContext context)
        {
            context.LogInfo($"Recevied S3 Event : {JsonConvert.SerializeObject(s3Event)}");

            // Validate the only required env variable has been set
            if (String.IsNullOrEmpty(_DestinationBucket))
            {
                string Message = "The environment variable DESTINATION_S3_BUCKET was not set.";
                context.LogError(Message);
                await SNSNotify(Message, context);
                return;
            }

            // Keep track of each copy task in this list
            List<Task<Manifest>> Tasks = new List<Task<Manifest>>();

            // Process each event record
            foreach (S3EventNotificationRecord Item in s3Event.Records)
            {
                if (ValidManifestFile(Item.S3.Object.Key))
                {
                    Tasks.Add(ProcessItemAsync(Item, _DestinationBucket, context));
                }
                else
                {
                    context.LogInfo($"The object s3://{Item.S3.Bucket.Name}/{Item.S3.Object.Key} is not a top level manifest file");
                }
            }

            // Process each copy task as it finishes
            foreach (Task<Manifest> Task in Tasks.Interleaved())
            {
                try
                {
                    Manifest Result = await Task;

                    if (Result == null)
                    {
                        string Message = "A task did not return successfully";
                        context.LogWarning(Message);
                        await SNSNotify(Message, context);
                    }
                    else
                    {
                        // Create or update the glue data catalog table
                        // for this CUR
                        string TableName = await CreateOrUpdateGlueTable(Result, context);

                        if (!String.IsNullOrEmpty(TableName))
                        {
                            // If provided, run a glue job
                            await RunGlueJob(TableName, context);
                        }
                        else
                        {
                            string Message = "The CreateOrUpdateGlueTable method returned an empty string for the table name, indicating either the DB or Table could not be created.";
                            context.LogWarning(Message);
                            await SNSNotify(Message, context);
                        }
                    }
                }
                catch (Exception e)
                {
                    string Message = "A process item async task failed with an exception.";
                    context.LogError(Message, e);
                    await SNSNotify(Message + $" {e.Message}", context);
                }
            }

            context.LogInfo("Function completed.");
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Creates or updates a glue table for the new CUR files. This makes sure any changes in the columns are captured
        /// and applied to the table. This will end up creating a new table for each billing period.
        /// </summary>
        /// <param name="manifest"></param>
        /// <param name="context"></param>
        /// <returns>The table name</returns>
        private static async Task<string> CreateOrUpdateGlueTable(Manifest manifest, ILambdaContext context)
        {
            if (String.IsNullOrEmpty(_GlueDatabaseName))
            {
                string Message = "No Glue database name defined, cannot create a table.";
                context.LogWarning(Message);
                await SNSNotify(Message, context);
                return String.Empty;
            }

            string Date = manifest.BillingPeriod.Start.ToString("yyyy-MM-dd");

            string Format = manifest.ContentType.ToLower().Substring(manifest.ContentType.LastIndexOf("/") + 1);

            Dictionary<string, string> Parameters;
            StorageDescriptor Descriptor;

            switch (Format)
            {
                case "csv":
                    {
                        Parameters = new Dictionary<string, string>()
                        {
                            { "EXTERNAL", "TRUE" },
                            { "skip.header.line.count", "1" },
                            { "columnsOrdered", "true" },
                            { "compressionType", manifest.Compression.ToString().ToLower() },
                            { "classification", manifest.ContentType.ToLower().Substring(manifest.ContentType.LastIndexOf("/") + 1) }
                        };

                        Descriptor = new StorageDescriptor()
                        {
                            Columns = manifest.Columns.Select(x => new Amazon.Glue.Model.Column() { Name = $"{x.Category}/{x.Name}", Type = (!String.IsNullOrEmpty(x.Type) ? x.Type : "string") }).ToList(),
                            InputFormat = "org.apache.hadoop.mapred.TextInputFormat",
                            OutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                            Location = $"s3://{_DestinationBucket}/{GetDestinationPrefix(manifest)}",
                            SerdeInfo = new SerDeInfo()
                            {
                                Name = "OpenCSVSerde",
                                SerializationLibrary = "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                                Parameters = new Dictionary<string, string>()
                                {
                                    { "escapeChar", "\\" },
                                    { "quoteChar", "\"" },
                                    { "separatorChar", "," }
                                }
                            }
                        };

                        break;
                    }
                case "parquet":
                    {
                        Parameters = new Dictionary<string, string>()
                        {
                            { "EXTERNAL", "TRUE" },
                            { "compressionType", manifest.Compression.ToString().ToLower() },
                            { "classification", manifest.ContentType.ToLower().Substring(manifest.ContentType.LastIndexOf("/") + 1) }
                        };

                        Descriptor = new StorageDescriptor()
                        {
                            Columns = manifest.Columns.Select(x => new Amazon.Glue.Model.Column() { Name = $"{x.Category}/{x.Name}", Type = (!String.IsNullOrEmpty(x.Type) ? x.Type : "string") }).ToList(),
                            InputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            OutputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            Location = $"s3://{_DestinationBucket}/{GetDestinationPrefix(manifest)}",
                            SerdeInfo = new SerDeInfo()
                            {
                                Name = "ParquetHiveSerDe",
                                SerializationLibrary = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                Parameters = new Dictionary<string, string>()
                                {
                                    { "serialization.format", "1" }
                                }
                            }
                        };
                        break;
                    }
                default:
                    {
                        string Message = $"Failed to create or update the database {_GlueDatabaseName} table. Unknown format type ${manifest.ContentType}.";
                        await SNSNotify(Message, context);
                        return String.Empty;
                    }
            }

            // The updated table input for this particular CUR
            TableInput TblInput = new TableInput()
            {
                Description = Date,
                Name = Date,
                TableType = "EXTERNAL_TABLE",
                Parameters = Parameters,
                StorageDescriptor = Descriptor
            };

            // Make sure the database exists
            GetDatabaseRequest GetDb = new GetDatabaseRequest()
            {
                Name = _GlueDatabaseName
            };

            try
            {
                await _GlueClient.GetDatabaseAsync(GetDb);
                context.LogInfo($"Database {_GlueDatabaseName} already exists.");
            }
            catch (EntityNotFoundException)
            {
                try
                {
                    CreateDatabaseRequest DbRequest = new CreateDatabaseRequest()
                    {
                        DatabaseInput = new DatabaseInput()
                        {
                            Name = _GlueDatabaseName
                        }
                    };

                    CreateDatabaseResponse Response = await _GlueClient.CreateDatabaseAsync(DbRequest);

                    if (Response.HttpStatusCode == HttpStatusCode.OK)
                    {
                        context.LogInfo($"Successfully CREATED database {_GlueDatabaseName}.");
                    }
                    else
                    {
                        context.LogError($"Failed to CREATE database with status code {(int)Response.HttpStatusCode}.");
                    }
                }
                catch (Exception ex)
                {
                    string Message = $"Failed to create the database {_GlueDatabaseName}.";
                    context.LogError(Message, ex);
                    await SNSNotify(Message + $" {ex.Message}", context);
                    return String.Empty;
                }
            }

            // Make sure the table exists
            GetTableRequest GetTable = new GetTableRequest()
            {
                DatabaseName = _GlueDatabaseName,
                Name = Date
            };

            try
            {
                GetTableResponse TableResponse = await _GlueClient.GetTableAsync(GetTable);

                UpdateTableRequest UpdateReq = new UpdateTableRequest()
                {
                    TableInput = TblInput,
                    DatabaseName = _GlueDatabaseName
                };

                UpdateTableResponse Response = await _GlueClient.UpdateTableAsync(UpdateReq);

                if (Response.HttpStatusCode == HttpStatusCode.OK)
                {
                    context.LogInfo($"Successfully UPDATED table {TblInput.Name} in database {_GlueDatabaseName}.");
                    return TblInput.Name;
                }
                else
                {
                    string Message = $"Failed to UPDATE table with status code {(int)Response.HttpStatusCode}.";
                    context.LogError(Message);
                    await SNSNotify(Message, context);
                    return String.Empty;
                }
            }
            catch (EntityNotFoundException) // This means the table does not exist
            {
                CreateTableRequest CreateReq = new CreateTableRequest()
                {
                    TableInput = TblInput,
                    DatabaseName = _GlueDatabaseName
                };

                CreateTableResponse Response = await _GlueClient.CreateTableAsync(CreateReq);

                if (Response.HttpStatusCode == HttpStatusCode.OK)
                {
                    context.LogInfo($"Successfully CREATED table {TblInput.Name} in database {_GlueDatabaseName}.");
                    return TblInput.Name;
                }
                else
                {
                    string Message = $"Failed to CREATE table with status code {(int)Response.HttpStatusCode}.";
                    context.LogError(Message);
                    await SNSNotify(Message, context);
                    return String.Empty;
                }
            }
        }

        /// <summary>
        /// If provided, runs a Glue job after the files have been copied
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async Task RunGlueJob(string table, ILambdaContext context)
        {
            if (String.IsNullOrEmpty(table))
            {
                throw new ArgumentNullException("table");
            }

            if (String.IsNullOrEmpty(_GlueDatabaseName))
            {
                string Message = "The Glue database name was provided. Not running job.";
                context.LogWarning(Message);
                await SNSNotify(Message, context);
                return;
            }

            if (String.IsNullOrEmpty(_GlueJobName))
            {
                string Message = "The Glue job name for the job was not provided as an environment variable. Not running job.";
                context.LogWarning(Message);
                await SNSNotify(Message, context);
                return;
            }

            context.LogInfo($"Running glue job on table {table} in database {_GlueDatabaseName}.");

            try
            {
                StartJobRunRequest Request = new StartJobRunRequest()
                {
                    JobName = _GlueJobName,
                    Timeout = 1440, // 24 Hours          
                    Arguments = new Dictionary<string, string>()
                        {
                            { "--table", table },
                            { "--database", _GlueDatabaseName }
                        }
                };

                if (!String.IsNullOrEmpty(_GlueDestinationBucket))
                {
                    Request.Arguments.Add("--destination_bucket", _GlueDestinationBucket);
                }

                StartJobRunResponse Response = await _GlueClient.StartJobRunAsync(Request);

                if (Response.HttpStatusCode != HttpStatusCode.OK)
                {
                    string Message = $"Failed to start job with status code ${(int)Response.HttpStatusCode}";
                    context.LogError(Message);
                    await SNSNotify(Message, context);
                }
                else
                {
                    context.LogInfo($"Successfully started job {Response.JobRunId}");
                }
            }
            catch (Exception e)
            {
                string Message = "Failed to start Glue job.";
                context.LogError(Message, e);
                await SNSNotify(Message + $" {e.Message}", context);
            }
        }

        /// <summary>
        /// If configured, sends an SNS notification to a topic
        /// </summary>
        /// <param name="message"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async Task SNSNotify(string message, ILambdaContext context)
        {
            if (!String.IsNullOrEmpty(_SNSTopic))
            {
                try
                {
                    PublishResponse Response = await _SNSClient.PublishAsync(_SNSTopic, message, _Subject);

                    if (Response.HttpStatusCode != HttpStatusCode.OK)
                    {
                        context.LogError($"Failed to send SNS notification with status code {(int)Response.HttpStatusCode}.");
                    }
                }
                catch (Exception e)
                {
                    context.LogError("Failed to send SNS notification.", e);
                }
            }
        }

        /// <summary>
        /// Validates that the manifest file is at the top level, not the one included with a CUR report delivery
        /// folder
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private static bool ValidManifestFile(string key)
        {
            // There are 2 different configurations, when new CUR report versions are created and when
            // they are overwritten. When they are overwritten, there's not a problem, but when new versions
            // are created the same ReportName-Manifest.json file is created in the sub directory.
            // Path for new versions at the top level
            // UserSpecificPrefix/ReportName/20190101-20190201/ReportName-Manifest.json
            // Path for new versions at the sub directory level
            // UserSpecificPrefix/ReportNAme/20180901-20181001/645ea520-4fa8-4e2a-99b1-e54de0817b66/ReportName-Manifest.json
            // This is mostly protected by the S3 event filter on the function that requires the suffix to be
            // 01/ReportName-Manifest.json, but the GUID could still end in 01, so we need an additional
            // level of protection, otherwise the function could get called twice and run the same actions twice
            // would could unecessarily run expensive Glue jobs

            // The key should not match the date and guid format regex
            return !_DateAndGuidRegex.IsMatch(key);
        }

        /// <summary>
        /// Processes a single manifest file and all of the report keys it contains
        /// </summary>
        /// <param name="item"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async Task<Manifest> ProcessItemAsync(S3EventNotificationRecord item, string destinationBucket, ILambdaContext context)
        {
            context.LogInfo(JsonConvert.SerializeObject(item));

            // Make sure the event was when a new object was created
            if (item.EventName != EventType.ObjectCreatedPut && item.EventName != EventType.ObjectCreatedPost)
            {
                string Message = $"This Lambda function was triggered by a non ObjectCreated Put or Post event, {item.EventName}, for object {item.S3.Object.Key}; check the CloudFormation template configuration and S3 Event setup.";
                context.LogWarning(Message);
                await SNSNotify(Message, context);
                return null;
            }

            // Get the manifest file contents
            GetObjectRequest Request = new GetObjectRequest()
            {
                BucketName = item.S3.Bucket.Name,
                Key = item.S3.Object.Key
            };

            string Body = "";

            using (GetObjectResponse Response = await _S3Client.GetObjectAsync(Request))
            {
                using (Stream ResponseStream = Response.ResponseStream)
                {
                    using (StreamReader Reader = new StreamReader(ResponseStream))
                    {
                        Body = await Reader.ReadToEndAsync();
                    }
                }
            }

            Manifest ManifestFile = Manifest.Build(Body);
            string Prefix = GetDestinationPrefix(ManifestFile);

            // Build the destination key map to link source key to destination key
            Dictionary<string, string> DestinationKeyMap = ManifestFile.ReportKeys.ToDictionary(x => x, x => $"{Prefix}/{Path.GetFileName(x)}");

            // If there are no destination keys
            // then there is nothing to do, return
            if (!DestinationKeyMap.Any())
            {
                string Message = $"No destination keys produced for s3://{Request.BucketName}/{Request.Key}";
                context.LogWarning(Message);
                await SNSNotify(Message, context);
                return null;
            }
          
            // Copy all of the files over first to replace existing files, this way there
            // is no period of time where a file may not exist and break an active query

            List<Task<CopyResponse>> CopyTasks = new List<Task<CopyResponse>>();

            // Initiate a copy object task for each key
            foreach (KeyValuePair<string, string> KeySet in DestinationKeyMap)
            {
                try
                {
                    context.LogInfo($"Copying CUR from s3://{item.S3.Bucket.Name}/{KeySet.Key} to s3://{_DestinationBucket}/{KeySet.Value}");
                    CopyTasks.Add(CopyObjectAsync(KeySet.Key, KeySet.Value, item.S3.Bucket.Name, _DestinationBucket));
                }
                catch (Exception e)
                {
                    string Message = $"Failed to add a copy object task to the queue for s3://{item.S3.Bucket.Name}/{KeySet.Key} to s3://{_DestinationBucket}/{KeySet.Value}.";
                    context.LogError(Message, e);
                    await SNSNotify(Message, context);
                    return null;
                }
            }

            // Process the copy object results
            foreach (Task<CopyResponse> Response in CopyTasks.Interleaved())
            {
                try
                {
                    CopyResponse Result = await Response;

                    if (Result.IsError)
                    {
                        string Message = $"Failed to copy s3://{Result.SourceBucket}/{Result.SourceKey} to s3://{Result.DestinationBucket}/{Result.DestinationKey}.";
                        context.LogError(Message, Result.Exception);
                        await SNSNotify(Message, context);
                        return null;
                    }
                    else
                    {
                        if (Result.Response.HttpStatusCode != HttpStatusCode.OK)
                        {
                            string Message = $"Failed to copy s3://{Result.SourceBucket}/{Result.SourceKey} to s3://{Result.DestinationBucket}/{Result.DestinationKey} with http code {(int)Result.Response.HttpStatusCode}.";
                            context.LogError(Message);
                            await SNSNotify(Message, context);
                            return null;
                        }
                        else
                        {
                            context.LogInfo($"Successfully copied CUR from s3://{Result.SourceBucket}/{Result.SourceKey} to s3://{Result.DestinationBucket}/{Result.DestinationKey}.");
                        }
                    }
                }
                catch (Exception e)
                {
                    string Message = $"Internal error processing the copy async task.";
                    context.LogError(Message, e);
                    await SNSNotify(Message, context);
                    return null;
                }
            }

            // Delete all of the keys in the that are not the files we just copied over

            List<KeyVersion> KeysToDelete;

            try
            {
                // Find all keys under the same prefix, and that aren't one of the keys of the files that have been copied
                KeysToDelete = await ListAllObjectsAsync(destinationBucket, Prefix, x => x.Where(y => !DestinationKeyMap.Values.Contains(y.Key)));
            }
            catch (Exception e)
            {
                context.LogError(e);
                await SNSNotify($"{e.Message}\n{e.StackTrace}", context);
                return null;
            }

            // Delete the old CUR files in the destination bucket
            try
            {
                if (KeysToDelete != null && KeysToDelete.Any())
                {
                    int DeletedCount = await DeleteObjectsAsync(KeysToDelete, destinationBucket);

                    if (DeletedCount != KeysToDelete.Count)
                    {
                        string Message = $"Unable to delete all objects, expected to delete {KeysToDelete.Count} but only deleted {DeletedCount}.";
                        context.LogError(Message);
                        await SNSNotify(Message, context);
                        return null;
                    }
                    else
                    {
                        context.LogInfo($"Successfully deleted {DeletedCount} objects.");
                    }
                }
            }
            catch (Exception e)
            {
                string Message = "Unable to delete all old CUR files.";
                context.LogError(Message, e);
                await SNSNotify(Message, context);
                return null;
            }

            return ManifestFile;
        }

        /// <summary>
        /// Chunks an IEnumerable into multiple lists of a specified size
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <param name="chunkSize"></param>
        /// <returns></returns>
        private static IEnumerable<List<T>> ChunkList<T>(IEnumerable<T> input, int chunkSize)
        {
            if (chunkSize <= 0)
            {
                throw new ArgumentOutOfRangeException("chunkSize", "The chunk size must be greater than 0.");
            }

            if (input == null)
            {
                throw new ArgumentNullException("input");
            }

            if (input.Any())
            {
                IEnumerator<T> Enumerator = input.GetEnumerator();
                List<T> ReturnList = new List<T>(chunkSize);
                int Counter = 1;

                while (Enumerator.MoveNext())
                {
                    if (Counter >= chunkSize)
                    {
                        yield return ReturnList;
                        ReturnList = new List<T>();
                        Counter = 1;
                    }

                    ReturnList.Add(Enumerator.Current);
                    Counter++;
                }

                yield return ReturnList;
            }
        }

        /// <summary>
        /// Gets the S3 prefix to be used in the destination bucket for this manifest's keys
        /// </summary>
        /// <param name="manifest"></param>
        /// <returns></returns>
        private static string GetDestinationPrefix(Manifest manifest)
        {
            string Prefix = String.Empty;
            DateTime Start = manifest.BillingPeriod.Start;

            switch (_OutputPathFormat)
            {
                case "billingperiod":
                    {
                        Prefix = $"accountid={manifest.Account}/billingperiod={Start.ToString("yyyy-MM-dd")}";
                        break;
                    }
                case "yearmonth":
                    {
                        Prefix = $"accountid={manifest.Account}/year={Start.Year}/month={Start.Month}";
                        break;
                    }
            }

            return Prefix;
        }

        /// <summary>
        /// Generates a map of the source report S3 keys to their destination S3 location
        /// based on the style of output path desired
        /// </summary>
        /// <param name="manifest"></param>
        /// <returns></returns>
        private static Dictionary<string, string> GetDestinationKeyMapping(Manifest manifest)
        {
            string Prefix = GetDestinationPrefix(manifest);

            return manifest.ReportKeys.ToDictionary(x => x, x => $"{Prefix}/{Path.GetFileName(x)}");
        }

        /// <summary>
        /// Lists all the objects in a bucket
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        private static async Task<List<KeyVersion>> ListAllObjectsAsync(string bucket, string prefix = "")
        {
            return await ListAllObjectsAsync(bucket, prefix, null);
        }

        /// <summary>
        /// Lists all the objects in a bucket
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        private static async Task<List<KeyVersion>> ListAllObjectsAsync(string bucket, Func<IEnumerable<S3Object>, IEnumerable<S3Object>> filter)
        {
            return await ListAllObjectsAsync(bucket, String.Empty, filter);
        }

        /// <summary>
        /// Lists all the objects in a bucket
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="filter"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        private static async Task<List<KeyVersion>> ListAllObjectsAsync(string bucket, string prefix = "", Func<IEnumerable<S3Object>, IEnumerable<S3Object>> filter = null)
        {
            List<KeyVersion> Keys = new List<KeyVersion>();

            ListObjectsV2Request Request = new ListObjectsV2Request()
            {
                BucketName = bucket
            };

            if (!String.IsNullOrEmpty(prefix))
            {
                Request.Prefix = prefix;
            }

            ListObjectsV2Response Response;

            do
            {
                Response = await _S3Client.ListObjectsV2Async(Request);

                if (Response.HttpStatusCode == HttpStatusCode.OK)
                {
                    // Only add keys that don't have the same Guid as our new file
                    if (filter != null)
                    {
                        Keys.AddRange(filter.Invoke(Response.S3Objects).Select(x => new KeyVersion() { Key = x.Key }));
                    }
                    else
                    {
                        Keys.AddRange(Response.S3Objects.Select(x => new KeyVersion() { Key = x.Key }));
                    }
                   
                    // Update the continuation token
                    Request.ContinuationToken = Response.NextContinuationToken;
                }
                else if (Response.HttpStatusCode == HttpStatusCode.ServiceUnavailable)
                {
                    // Implement linear backoff
                    Thread.Sleep(2000);
                }
                else
                {
                    throw new Exception($"Could not retrieve data from S3 Bucket {bucket} with status : {(int)Response.HttpStatusCode}.");
                }

            } while (Response.IsTruncated);

            return Keys;
        }

        /// <summary>
        /// Deletes all of the supplied keys in the specified bucket
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="bucket"></param>
        /// <returns></returns>
        private static async Task<int> DeleteObjectsAsync(IEnumerable<KeyVersion> keys, string bucket)
        {
            int Counter = 0;

            // Delete all of the files, 1000 at a time, that have already been delivered for this billing period,
            // but are not part of this CUR's chunk set (we filtered out the GUID earlier)
            foreach (List<KeyVersion> Chunk in ChunkList(keys, 1000))
            {
                DeleteObjectsRequest DeleteRequest = new DeleteObjectsRequest()
                {
                    BucketName = bucket,
                    Objects = Chunk
                };

                while (true)
                {
                    DeleteObjectsResponse DeleteResponse = await _S3Client.DeleteObjectsAsync(DeleteRequest);

                    if (DeleteResponse.HttpStatusCode == HttpStatusCode.OK)
                    {
                        Counter += DeleteResponse.DeletedObjects.Count;
                        break;
                    }
                    else if (DeleteResponse.HttpStatusCode == HttpStatusCode.ServiceUnavailable)
                    {
                        // Use linear backoff
                        Thread.Sleep(2000);
                    }
                    else
                    {
                        string Message = String.Join("\n", DeleteResponse.DeleteErrors.Select(x => $"{x.Key} = {x.Code} : {x.Message}"));

                        throw new Exception($"Could not delete objects from S3 with status {(int)DeleteResponse.HttpStatusCode} and errors:\n{Message}");
                    }
                }
            }

            return Counter;
        }

        /// <summary>
        /// Copies the key values in the dictionary to their mapped value from the source bucket to the
        /// destination bucket
        /// </summary>
        /// <param name="keyMap">A mapping of the source key to destination key</param>
        /// <param name="sourceBucket"></param>
        /// <param name="destinationBucket"></param>
        /// <returns></returns>
        private static async Task<CopyResponse> CopyObjectAsync(string sourceKey, string destinationKey, string sourceBucket, string destinationBucket)
        {
            CopyObjectRequest CopyRequest = new CopyObjectRequest()
            {
                SourceBucket = sourceBucket,
                SourceKey = sourceKey,
                DestinationBucket = _DestinationBucket,
                DestinationKey = destinationKey
            };

            try
            {
                while (true)
                {     
                    CopyObjectResponse CopyResponse = await _S3Client.CopyOrMoveObjectAsync(CopyRequest);

                    if (CopyResponse.HttpStatusCode == HttpStatusCode.OK)
                    {
                        return new CopyResponse(CopyResponse, sourceBucket, sourceKey, destinationBucket, destinationKey);
                    }
                    else if (CopyResponse.HttpStatusCode == HttpStatusCode.ServiceUnavailable)
                    {
                        // Use a linear backoff
                        Thread.Sleep(2000);
                    }
                    else
                    {
                        return new CopyResponse(new Exception($"Received an HTTP {(int)CopyResponse.HttpStatusCode} {CopyResponse.HttpStatusCode} response"), sourceBucket, sourceKey, destinationBucket, destinationKey);
                    }
                } 
            }
            catch (Exception e)
            {
                return new CopyResponse(e, sourceBucket, sourceKey, destinationBucket, destinationKey);
            }
        }

        #endregion

        #region Private Classes

        /// <summary>
        /// Represents a response from a copy object API call with additional
        /// properties to include source and destination information
        /// </summary>
        private class CopyResponse
        {
            #region Public Properties

            public CopyObjectResponse Response { get; }

            public string SourceBucket { get; }

            public string SourceKey { get; }

            public string DestinationBucket { get; }

            public string DestinationKey { get; }

            public Exception Exception { get; }

            public bool IsError
            {
                get
                {
                    return this.Exception != null;
                }
            }

            #endregion

            #region Constructors

            public CopyResponse(CopyObjectResponse response,
                string sourceBucket,
                string sourceKey,
                string destinationBucket,
                string destinationKey)
            {
                this.Response = response;
                this.SourceBucket = sourceBucket;
                this.SourceKey = sourceKey;
                this.DestinationBucket = destinationBucket;
                this.DestinationKey = destinationKey;
                this.Exception = null;
            }

            public CopyResponse(Exception exception,
               string sourceBucket,
               string sourceKey,
               string destinationBucket,
               string destinationKey)
            {
                this.Response = null;
                this.SourceBucket = sourceBucket;
                this.SourceKey = sourceKey;
                this.DestinationBucket = destinationBucket;
                this.DestinationKey = destinationKey;
                this.Exception = exception;
            }

            #endregion
        }

        #endregion
    }
}