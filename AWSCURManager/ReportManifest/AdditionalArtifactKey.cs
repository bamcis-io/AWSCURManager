using Newtonsoft.Json;
using System;

namespace BAMCIS.LambaFunctions.AWSCURManager.ReportManifest
{
    /// <summary>
    /// Represents an additional artifact key in the manifest
    /// </summary>
    public class AdditionalArtifactKey
    {
        #region Public Properties

        /// <summary>
        /// The artifact type, like RedshiftCommands or RedshiftManifest
        /// </summary>
        public string ArtifactType { get; }

        /// <summary>
        /// The actual S3 key to the object
        /// </summary>
        public string Name { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new additional artifact key
        /// </summary>
        /// <param name="artifactType"></param>
        /// <param name="name"></param>
        [JsonConstructor()]
        public AdditionalArtifactKey(string artifactType, string name)
        {
            this.ArtifactType = artifactType ?? throw new ArgumentNullException("artifactType");
            this.Name = name ?? throw new ArgumentNullException("name");
        }

        #endregion
    }
}
