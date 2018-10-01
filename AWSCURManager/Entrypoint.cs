using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using BAMCIS.AWSLambda.Common;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static Amazon.S3.Util.S3EventNotification;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BAMCIS.LambdaFunctions.CURManager
{
    /// <summary>
    /// Entrypoint class for the Lambda function
    /// </summary>
    public class Entrypoint
    {
        #region Private Fields

        private static IAmazonS3 _S3Client;
        private static string _YearMonthDay = "20[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[1-2][0-9]|3[0-1])";
        private static string _Guid = "{?[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[0-9a-f]{4}-?[0-9a-f]{12}}?";

        private static Regex _DateAndGuidRegex = new Regex($"({_YearMonthDay})-{_YearMonthDay}/({_Guid})", RegexOptions.IgnoreCase);
        private static Regex _ExtensionRegex = new Regex(@"(\.csv(?:\.gz|\.zip)?)$");
        private static string _DestinationBucket;

        #endregion

        #region Constructors

        /// <summary>
        /// Static constructor
        /// </summary>
        static Entrypoint()
        {
            _S3Client = new AmazonS3Client();
            _DestinationBucket = Environment.GetEnvironmentVariable("DESTINATION_S3_BUCKET");
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
        /// Deletes all S3 files for the same billing period that are already in S3 except for the
        /// object that caused this function to be run
        /// </summary>
        /// <param name="s3Event"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task Exec(S3Event s3Event, ILambdaContext context)
        {
            context.LogInfo($"Recevied S3 Event : {JsonConvert.SerializeObject(s3Event)}");

            if (String.IsNullOrEmpty(_DestinationBucket))
            {
                context.LogError("The environment variable DESTINATION_S3_BUCKET was not set.");
                return;
            }

            foreach (S3EventNotificationRecord Item in s3Event.Records)
            {
                context.LogInfo(JsonConvert.SerializeObject(Item));

                string Bucket = Item.S3.Bucket.Name;
                string Key = Item.S3.Object.Key;

                // Make sure the event was when a new object was created
                if (Item.EventName == EventType.ObjectCreatedAll)
                {
                    Match ExtensionMatch = _ExtensionRegex.Match(Key);

                    // Make sure the object is a csv, csv.gz or csv.zip file
                    if (ExtensionMatch.Success)
                    {
                        // Extract the date section of the key prefix
                        Match DateGuidMatch = _DateAndGuidRegex.Match(Key);

                        if (DateGuidMatch.Success)
                        {
                            // This is the whole 
                            string DateRangePrefix = DateGuidMatch.Groups[1].Value;
                            string Guid = DateGuidMatch.Groups[2].Value;

                            List<KeyVersion> Keys = new List<KeyVersion>();

                            string AccountId = context.InvokedFunctionArn.Split(":")[4];

                            // accountid=123456789012/date=20181001/eb3c690f-eeaa-4781-b701-4fd32f8ab19f.csv.gz
                            // This leaves a string.Format option in case the CUR report is chunked
                            string[] KeyParts = Path.GetFileNameWithoutExtension(Key).Split("-");

                            string ChunkId = "";

                            if (KeyParts.Length > 1)
                            {
                                // Get file name without extension may only truncate off the .gz part from
                                // .csv.gz or .csv.zip, so we know the actual chunk id will be the 0 element
                                ChunkId = $"-{KeyParts[1].Split(".")[0]}";
                            }
                            
                            string DestinationKey = $"accountid={AccountId}/date={DateGuidMatch.Groups[1]}/{DateGuidMatch.Groups[2]}{ChunkId}{ExtensionMatch.Groups[1]}";

                            ListObjectsV2Request Request = new ListObjectsV2Request()
                            {
                                BucketName = _DestinationBucket,
                                Prefix = DestinationKey.Substring(0, DestinationKey.LastIndexOf("/"))

                            };

                            ListObjectsV2Response Response;

                            do
                            {
                                try
                                {
                                    Response = await _S3Client.ListObjectsV2Async(Request);

                                    if (Response.HttpStatusCode == HttpStatusCode.OK)
                                    {
                                        // Only add keys that don't have the same Guid as our new file
                                        Keys.AddRange(Response.S3Objects.Where(x => !Path.GetFileName(x.Key).StartsWith(Guid)).Select(x => new KeyVersion() { Key = x.Key }));

                                        // Update the continuation token
                                        Request.ContinuationToken = Response.NextContinuationToken;
                                    }
                                    else
                                    {
                                        context.LogError($"Could not retrieve data from S3 Bucket {Bucket} with status : {(int)Response.HttpStatusCode}.");
                                        return;
                                    }
                                }
                                catch (Exception e)
                                {
                                    string Type = e.GetType().FullName;
                                    context.LogError(e);
                                    return;
                                }
                            } while (Response.IsTruncated);

                            // Delete all of the files, 1000 at a time, that have already been delivered for this billing period,
                            // but are not part of this CUR's chunk set (we filtered out the GUID earlier)
                            foreach (List<KeyVersion> keys in ChunkList(Keys.Where(x => !x.Key.Equals(Key)), 1000))
                            {
                                try
                                {
                                    DeleteObjectsRequest DeleteRequest = new DeleteObjectsRequest()
                                    {
                                        BucketName = Bucket,
                                        Objects = keys
                                    };

                                    DeleteObjectsResponse DeleteResponse = await _S3Client.DeleteObjectsAsync(DeleteRequest);

                                    if (DeleteResponse.HttpStatusCode == HttpStatusCode.OK)
                                    {
                                        context.LogInfo($"Successfully deleted {DeleteResponse.DeletedObjects.Count} objects.");
                                    }
                                    else
                                    {
                                        string Message = String.Join("\n", DeleteResponse.DeleteErrors.Select(x => $"{x.Key} = {x.Code} : {x.Message}"));
                                        context.LogError($"Could not delete objects from S3 with status {(int)DeleteResponse.HttpStatusCode} and errors:\n{Message}");
                                    }
                                }
                                catch (Exception e)
                                {
                                    context.LogError("Failed to delete all CUR files.", e);
                                    return;
                                }
                            }

                            // Copy this file into the new bucket with its new name
                            try
                            {
                                context.LogInfo($"Copying CUR from s3://{Bucket}/{Key} to s3://{_DestinationBucket}/{DestinationKey}");

                                CopyObjectRequest CopyRequest = new CopyObjectRequest()
                                {
                                    SourceBucket = Bucket,
                                    SourceKey = Key,
                                    DestinationBucket = _DestinationBucket,
                                    DestinationKey = DestinationKey
                                };

                                CopyObjectResponse CopyResponse = await _S3Client.CopyOrMoveObjectAsync(CopyRequest);

                                context.LogInfo("Successfully moved CUR file.");
                            }
                            catch (Exception e)
                            {
                                context.LogError($"Failed to move s3://{Bucket}/{Key}", e);
                            }
                        }
                        else
                        {
                            context.LogWarning($"Did not find a Date string match in {Key}.");
                        }
                    }
                    else
                    {
                        context.LogInfo($"Ignoring S3 file {Key}, did not match extension regex.");
                    }
                }
                else
                {
                    context.LogWarning($"This Lambda function was triggered by a non ObjectCreated event, {Item.EventName}, for object {Item.S3.Object.Key}; check the CloudFormation template configuration and S3 Event setup.");
                }
            }
        }

        #endregion

        #region Private Methods

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

        #endregion
    }
}
