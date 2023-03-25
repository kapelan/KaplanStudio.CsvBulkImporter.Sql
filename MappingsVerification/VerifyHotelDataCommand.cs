using System.Data.SqlClient;
using System.Globalization;
using System.Threading.Channels;
using BulkInsert.MappingsVerification.Data;
using CsvHelper;
using Microsoft.Extensions.Configuration;
using Oakton;

namespace BulkInsert.MappingsVerification
{
    [Description("Verify date from CSV cache file against static data from db")]
    public class VerifyHotelDataCommand : OaktonAsyncCommand<VerifyHotelDataInput>
    {
        private const string _hotelFileName = "test.csv";

        private readonly Channel<ReportData> _reportChannel;
        private readonly Channel<WBRoomData> _uploadChannel;
        private readonly ReportBuilder _reportBuilder;
        private readonly Task _reportBuilderTask;

        public VerifyHotelDataCommand()
        {
            Usage("Default").Arguments();
            _reportChannel = Channel.CreateUnbounded<ReportData>();
            _uploadChannel = Channel.CreateUnbounded<WBRoomData>();
            //_dataProcessor = new DataProcessor(_reportChannel.Writer);
            _reportBuilder = new ReportBuilder(_reportChannel.Reader);
            _reportBuilderTask = Task.Run(() => _reportBuilder.RunAsync());
        }

        public override async Task<bool> Execute(VerifyHotelDataInput input)
        {
            ConsoleWriter.Write(ConsoleColor.Green, $"[{DateTime.Now.ToShortTimeString()}]Hotels upload started");
            await CleanUp();
            await UploadHotelsDataAsync();
            ConsoleWriter.Write(ConsoleColor.Green, $"[{DateTime.Now.ToShortTimeString()}]Hotels upload finished");
            ConsoleWriter.Write(ConsoleColor.Green, $"[{DateTime.Now.ToShortTimeString()}]Rooms upload started");
            await UploadRoomsDataAsync();
            ConsoleWriter.Write(ConsoleColor.Green, $"[{DateTime.Now.ToShortTimeString()}]Rooms upload finished");
            ConsoleWriter.Write(ConsoleColor.Green, "Mapping verification  started");
            await VerifyStaticDataAsync();
            ConsoleWriter.Write(ConsoleColor.Green, "Completed! Reports has been saved.");
            return true;
        }

        private async Task VerifyStaticDataAsync(CancellationToken ctk = default)
        {
        }

        private async Task CleanUp(CancellationToken ctk = default)
        {
            using (var connection = new SqlConnection(GetConnectionString()))
            {
                await connection.OpenAsync(ctk);

                var hotelSchemaProvider = new TableSchemaProvider(connection, "WBHotelData");
                var roomSchemaProvider = new TableSchemaProvider(connection, "WBRoomData");

                if (await roomSchemaProvider.TableExistsAsync()) await roomSchemaProvider.DropTableAsync();
                if (await hotelSchemaProvider.TableExistsAsync()) await hotelSchemaProvider.DropTableAsync();

                await hotelSchemaProvider.ExecuteScriptAsync(CreateHotelTableCommand);
                await roomSchemaProvider.ExecuteScriptAsync(CreateRoomTableCommand);
            }
        }

        private async Task UploadHotelsDataAsync(CancellationToken ctk = default)
        {
            var appSettingsPath = Directory.GetCurrentDirectory();
            var hotelFileLocation = Path.Combine(appSettingsPath, _hotelFileName);
            using (var reader = new StreamReader(hotelFileLocation))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            using (var connection = new SqlConnection(GetConnectionString()))
            {
                await connection.OpenAsync(ctk);
                csv.Context.RegisterClassMap<WBHotelDataMap>();
                var data = csv.GetRecordsAsync<WBHotelData>(ctk);

                var schemaProvider = new TableSchemaProvider(connection, "WBHotelData");

                var fields = await schemaProvider.GetFieldsAsync();

                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "WBHotelData";
                    bulkCopy.BatchSize = 10000;
                    bulkCopy.BulkCopyTimeout = (int) TimeSpan.FromMinutes(10).TotalSeconds;

                    foreach (var field in fields)
                        bulkCopy.ColumnMappings.Add(field.FieldName, field.FieldName);

                    using (var dataReader = new GenericDataReader<WBHotelData>(data, fields))
                    {
                        await bulkCopy.WriteToServerAsync(dataReader, ctk);
                    }
                }
            }
        }

        private async Task UploadRoomsDataAsync(CancellationToken ctk = default)
        {
            var appSettingsPath = Directory.GetCurrentDirectory();
            var hotelFileLocation = Path.Combine(appSettingsPath, _hotelFileName);
            using (var reader = new StreamReader(hotelFileLocation))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                var uploader = new RoomDataUploader(_uploadChannel.Reader, GetConnectionString());
                var uploadTask = Task.Run(() => uploader.RunAsync(ctk));
                var producer = _uploadChannel.Writer;
                csv.Context.RegisterClassMap<WBHotelDataMap>();
                var data = csv.GetRecordsAsync<WBHotelData>(ctk);
                await foreach (var hotelData in data)
                {
                    foreach (var room in hotelData.WBRoomTypes.Split('|')
                                 .Select(x => WBRoomData.CreateWBRoomData(hotelData.WBId, x)))
                    {
                        await producer.WriteAsync(room, ctk);
                    }
                }

                producer.Complete();
                await Task.WhenAll(uploadTask);
            }
        }

        private string GetConnectionString()
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();

            return configuration.GetValue<string>("DB_CONNECTION_STRING");
        }

        private string CreateHotelTableCommand =>
            "If not exists (select name from sysobjects where name = 'WBHotelData') CREATE TABLE WBHotelData(WBId int NOT NULL,WBName nvarchar(200),WBLatitude nvarchar(50),WBLongitude nvarchar(50),WBRoomTypes nvarchar(50),WBAccommodationType nvarchar(MAX),PRIMARY KEY (WBId))";

        private string CreateRoomTableCommand =>
            "If not exists (select name from sysobjects where name = 'WBRoomData') CREATE TABLE WBRoomData(WBRoomId int NOT NULL, WBHotelId int NOT NULL,WBRoomTypeId int NOT NULL,WBBeds int NOT NULL,WBExtraBeds int NOT NULL,WBIsBestBuy bit, PRIMARY KEY (WBRoomId), FOREIGN KEY (WBHotelId) REFERENCES WBHotelData(WBId));";
    }
}