using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace BulkInsert.MappingsVerification.Data
{
    public class WBHotelData
    {
        public int WBId { get; set; }
        public string WBName { get; set; }
        public string WBLatitude { get; set; }
        public string WBLongitude { get; set; }
        public string WBAccommodationType { get; set; }
        public string WBRoomTypes { get; set; }
    }

    public class WBIdentityConverter : DefaultTypeConverter
    {
        public override object ConvertFromString(string text, IReaderRow row, MemberMapData memberMapData)
        {
            var ans = int.Parse(text);
            return ans;
        }

        public override string ConvertToString(object value, IWriterRow row, MemberMapData memberMapData)
        {
            return ((int) value).ToString();
        }
    }

    public sealed class WBHotelDataMap : ClassMap<WBHotelData>
    {
        public WBHotelDataMap()
        {   
            Map(m => m.WBLatitude);
            Map(m => m.WBLongitude);
            Map(m => m.WBAccommodationType);
            Map(m => m.WBRoomTypes);
            Map(m => m.WBName);
            Map(m => m.WBId).TypeConverter<WBIdentityConverter>();
        }
    }
}