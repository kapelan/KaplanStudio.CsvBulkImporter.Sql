using System.Globalization;
using System.Threading.Channels;
using BulkInsert.MappingsVerification.Data;
using CsvHelper;

namespace BulkInsert.MappingsVerification
{
    public class ReportBuilder : IDisposable
    {
        private readonly ChannelReader<ReportData> _channel;

        private const string ReportCsvFilename = "report.csv";
        private readonly CsvWriter _reportHotelsCsvWriter;

        public ReportBuilder(ChannelReader<ReportData> channel)
        {
            _channel = channel;
            _reportHotelsCsvWriter = CreateCsvWriter(ReportCsvFilename);
        }

        public async Task RunAsync(CancellationToken stoppingToken = default)
        {
            await _reportHotelsCsvWriter.WriteRecordsAsync(_channel.ReadAllAsync(stoppingToken), stoppingToken);
            await _reportHotelsCsvWriter.FlushAsync();
        }

        private static CsvWriter CreateCsvWriter(string fileName)
        {
            var writer = new StreamWriter($"{Directory.GetCurrentDirectory()}/{fileName}");
            var csv = new CsvWriter(writer, CultureInfo.InvariantCulture, true);
            return csv;
        }

        public void Dispose()
        {
            _reportHotelsCsvWriter.Dispose();
        }
    }
}