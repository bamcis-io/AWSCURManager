using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;

namespace BAMCIS.LambaFunctions.AWSCURManager.ReportManifest
{
    /// <summary>
    /// Represents the start and end of the billing period in a manifest file
    /// </summary>
    public class BillingPeriod
    {
        #region Public Properties

        /// <summary>
        /// The start of the billing period
        /// </summary>
        [JsonConverter(typeof(ManifestDateTimeConverter))]
        public DateTime Start { get; }

        /// <summary>
        /// The end of the billing period
        /// </summary>
        [JsonConverter(typeof(ManifestDateTimeConverter))]
        public DateTime End { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new billing period object
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        [JsonConstructor()]
        public BillingPeriod(DateTime start, DateTime end)
        {
            this.Start = start;
            this.End = end;
        }

        #endregion

        #region Public Properties

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

            BillingPeriod Other = (BillingPeriod)obj;

            return this.Start == Other.Start &&
                this.End == Other.End;
        }

        public override int GetHashCode()
        {
            return Hashing.Hash(
                this.Start,
                this.End
            );
        }

        public static bool operator ==(BillingPeriod left, BillingPeriod right)
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

        public static bool operator !=(BillingPeriod left, BillingPeriod right)
        {
            return !(left == right);
        }

        #endregion
    }
}
