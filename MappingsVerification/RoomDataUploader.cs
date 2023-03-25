using System.Threading.Channels;
using BulkInsert.MappingsVerification.Data;
using Microsoft.Data.SqlClient;

namespace BulkInsert.MappingsVerification
{
    public class RoomDataUploader : IDisposable
    {
        private readonly ChannelReader<WBRoomData> _channel;
        private readonly SqlConnection _connection;

        public RoomDataUploader(ChannelReader<WBRoomData> channel, string connectionString)
        {
            _channel = channel;
            _connection = new SqlConnection(connectionString);
        }

        public async Task RunAsync(CancellationToken ctk = default)
        {
            await _connection.OpenAsync(ctk);

            var schemaProvider = new TableSchemaProvider(_connection, "WBRoomData");
            
            var fields = await schemaProvider.GetFieldsAsync();

            using (var bulkCopy = new SqlBulkCopy(_connection))
            {
                bulkCopy.DestinationTableName = "WBRoomData";
                bulkCopy.BatchSize = 10000;
                bulkCopy.BulkCopyTimeout = (int) TimeSpan.FromMinutes(10).TotalSeconds;

                foreach (var field in fields)
                    bulkCopy.ColumnMappings.Add(field.FieldName, field.FieldName);

                using (var dataReader = new GenericDataReader<WBRoomData>(_channel.ReadAllAsync(ctk), fields))
                {
                    await bulkCopy.WriteToServerAsync(dataReader, ctk);
                }
            }
        }

        public void Dispose()
        {
            _connection.Dispose();
        }
    }
}