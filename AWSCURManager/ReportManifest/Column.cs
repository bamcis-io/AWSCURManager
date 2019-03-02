using Newtonsoft.Json;
using System;

namespace BAMCIS.LambaFunctions.AWSCURManager.ReportManifest
{
    /// <summary>
    /// Represents a column identifier in a manifest file
    /// </summary>
    public class Column
    {
        #region Public Properties

        /// <summary>
        ///  The column category like Identity or LineItem
        /// </summary>
        public string Category { get; }

        /// <summary>
        /// The column name
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The column type, like string or DateTime. If the type wasn't defined in the manifest,
        /// then this value may be null
        /// </summary>
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new column object
        /// </summary>
        /// <param name="category"></param>
        /// <param name="name"></param>
        /// <param name="type"></param>
        [JsonConstructor()]
        public Column(string category, string name, string type)
        {
            this.Category = category ?? throw new ArgumentNullException("category");
            this.Name = name ?? throw new ArgumentNullException("name");
            this.Type = type;
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

            Column Other = (Column)obj;

            return this.Category == Other.Category &&
                this.Name == Other.Name;
        }

        public override int GetHashCode()
        {
            return Hashing.Hash(
                this.Category,
                this.Name
            );
        }

        public static bool operator ==(Column left, Column right)
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

        public static bool operator !=(Column left, Column right)
        {
            return !(left == right);
        }


        #endregion
    }
}
