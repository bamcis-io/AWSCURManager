using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace BAMCIS.LambaFunctions.AWSCURManager.ReportManifest
{
    public class ManifestDateTimeConverter : JsonConverter
    {
        private string Format = "20181001T000000.000Z";

        #region Public Properties

        public override bool CanRead => true;

        public override bool CanWrite => true;

        #endregion

        #region Public Methods

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(DateTime);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            string Value = reader.Value.ToString();

            if (DateTime.TryParseExact(Value, "yyyyMMddTHHmmss.fffZ", null, System.Globalization.DateTimeStyles.AssumeUniversal, out DateTime Result))
            {
                return Result.ToUniversalTime();
            }
            else
            {
                return DateTime.MinValue;
            }
            
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(((DateTime)value).ToUniversalTime().ToString("yyyyMMddTHHmmss.fffZ"));
        }

        #endregion
    }
}
