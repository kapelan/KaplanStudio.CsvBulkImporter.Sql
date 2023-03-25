namespace BulkInsert.MappingsVerification.Data
{
    public class WBRoomData
    {
        public int WBRoomId { get; set; }
        public int WBHotelId { get; set; }
        public int WBRoomTypeId { get; set; }
        public int WBBeds { get; set; }
        public int WBExtraBeds { get; set; }
        public bool WBIsBestBuy { get; set; }

        //example: 4133.2.0/28860111/False
        public static bool TryCreateWBRoomData(int HotelId, string roomData, out WBRoomData? room)
        {
            room = null;

            var roomDataSplit = roomData.Split('/');
            if (roomDataSplit.Length != 3) return false;

            var roomTypeSplit = roomDataSplit[0].Split('.');
            if (roomTypeSplit.Length != 3) return false;
            
            room = new WBRoomData
            {
                WBHotelId = HotelId,
                WBRoomId = int.Parse(roomDataSplit[1]),
                WBRoomTypeId = int.Parse(roomTypeSplit[0]),
                WBBeds = int.Parse(roomTypeSplit[1]),
                WBExtraBeds = int.Parse(roomTypeSplit[2]),
                WBIsBestBuy = bool.Parse(roomDataSplit[2])
            };
            return true;
        }
        
        public static WBRoomData CreateWBRoomData(int HotelId, string roomData)
        {
            var roomDataSplit = roomData.Split('/');
            var roomTypeSplit = roomDataSplit[0].Split('.');

            return new WBRoomData
            {
                WBHotelId = HotelId,
                WBRoomId = int.Parse(roomDataSplit[1]),
                WBRoomTypeId = int.Parse(roomTypeSplit[0]),
                WBBeds = int.Parse(roomTypeSplit[1]),
                WBExtraBeds = int.Parse(roomTypeSplit[2]),
                WBIsBestBuy = bool.Parse(roomDataSplit[2])
            };
        }
    }
    
    
}