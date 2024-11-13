using Azure;

namespace BWJ.Core.CosmosRepository
{
    public abstract class BusinessEntity : IBusinessEntity
    {
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
        public string? __OriginalPartitionKey { get; set; }
    }
}
