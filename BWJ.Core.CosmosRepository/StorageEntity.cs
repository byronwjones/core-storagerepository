using Azure;
using Azure.Data.Tables;

namespace BWJ.Core.CosmosRepository
{
    public abstract class StorageEntity : ITableEntity
    {
        protected abstract string GetPartitionKey();
        protected abstract string GetRowKey();

        public string PartitionKey { get => GetPartitionKey(); set { } }
        public string RowKey { get => GetRowKey(); set { } }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
    }
}