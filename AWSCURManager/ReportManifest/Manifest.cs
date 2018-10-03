using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BAMCIS.LambaFunctions.AWSCURManager.ReportManifest
{
    public class Manifest
    {
        #region Public Properties

        public Guid AssemblyId { get; }

        public string Account { get; }

        public IEnumerable<Column> Columns { get; }

        public string Charset { get; }

        public Compression Compression { get; }

        public string ContentType { get; }

        public string ReportId { get; }

        public string ReportName { get; }

        public BillingPeriod BillingPeriod { get; }

        public string Bucket { get; }

        public IEnumerable<string> ReportKeys { get; }

        public IEnumerable<string> AdditionalArtifactKeys { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new manifest file
        /// </summary>
        /// <param name="assemblyId"></param>
        /// <param name="account"></param>
        /// <param name="columns"></param>
        /// <param name="charset"></param>
        /// <param name="compression"></param>
        /// <param name="contentType"></param>
        /// <param name="reportId"></param>
        /// <param name="reportName"></param>
        /// <param name="billingPeriod"></param>
        /// <param name="bucket"></param>
        /// <param name="reportKeys"></param>
        /// <param name="additionalArtifactKeys"></param>
        [JsonConstructor()]
        public Manifest(
            Guid assemblyId,
            string account,
            IEnumerable<Column> columns,
            string charset,
            Compression compression,     
            string contentType,
            string reportId,
            string reportName,
            BillingPeriod billingPeriod,
            string bucket,
            IEnumerable<string> reportKeys,
            IEnumerable<string> additionalArtifactKeys
            )
        {
            this.AssemblyId = assemblyId;
            this.Account = account ?? throw new ArgumentNullException("account");
            this.Columns = columns ?? throw new ArgumentNullException("columns");
            this.Charset = charset ?? throw new ArgumentNullException("charset");
            this.Compression = compression;
            this.ContentType = contentType ?? throw new ArgumentNullException("contentType");
            this.ReportId = reportId ?? throw new ArgumentNullException("reportId");
            this.ReportName = reportName ?? throw new ArgumentNullException("reportName");
            this.BillingPeriod = billingPeriod ?? throw new ArgumentNullException("billingPeriod");
            this.Bucket = bucket ?? throw new ArgumentNullException("bucket");
            this.ReportKeys = reportKeys ?? throw new ArgumentNullException("reportKeys");
            this.AdditionalArtifactKeys = additionalArtifactKeys;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Deserializes the json into a manifest object
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static Manifest Build(string json)
        {
            if (String.IsNullOrEmpty(json))
            {
                throw new ArgumentNullException("json");
            }

            return JsonConvert.DeserializeObject<Manifest>(json);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj == null || this.GetType() != obj.GetType())
            {
                return false;
            }

            Manifest Other = (Manifest)obj;

            return this.Account == Other.Account &&
                Enumerable.SequenceEqual(this.AdditionalArtifactKeys, Other.AdditionalArtifactKeys) &&
                this.AssemblyId == Other.AssemblyId &&
                this.BillingPeriod == Other.BillingPeriod &&
                this.Bucket == Other.Bucket &&
                this.Charset == Other.Charset &&
                this.Columns == Other.Columns &&
                this.Compression == Other.Compression &&
                this.ContentType == Other.ContentType &&
                this.ReportId == Other.ReportId &&
                Enumerable.SequenceEqual(this.ReportKeys, Other.ReportKeys) &&
                this.ReportName == Other.ReportName;
        }

        public override int GetHashCode()
        {
            return Hashing.Hash(
                this.Account, 
                this.AdditionalArtifactKeys, 
                this.AssemblyId, 
                this.BillingPeriod,
                this.Bucket,
                this.Charset,
                this.Columns,
                this.Compression,
                this.ContentType,
                this.ReportId,
                this.ReportKeys,
                this.ReportName
            );
        }

        public static bool operator ==(Manifest left, Manifest right)
        {
            if (ReferenceEquals(left, right))
            {
                return true;
            }

            if (right is null || left is null)
            {
                return false;
            }

            return left.Equals(right);
        }

        public static bool operator !=(Manifest left, Manifest right)
        {
            return !(left == right);
        }

        #endregion
    }
}
