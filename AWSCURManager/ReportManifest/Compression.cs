namespace BAMCIS.LambaFunctions.AWSCURManager.ReportManifest
{
    /// <summary>
    /// The compression types for the delivered CUR
    /// </summary>
    public enum Compression
    {
        /// <summary>
        /// GZIP compression
        /// </summary>
        GZIP,

        /// <summary>
        /// ZIP compression
        /// </summary>
        ZIP,

        /// <summary>
        /// No compression
        /// </summary>
        NONE,

        /// <summary>
        /// Parquet compression
        /// </summary>
        PARQUET
    }
}
