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

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new column object
        /// </summary>
        /// <param name="category"></param>
        /// <param name="name"></param>
        [JsonConstructor()]
        public Column(string category, string name)
        {
            this.Category = category ?? throw new ArgumentNullException("category");
            this.Name = name ?? throw new ArgumentNullException("name");
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
