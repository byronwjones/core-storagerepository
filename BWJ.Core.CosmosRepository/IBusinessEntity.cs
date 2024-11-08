using Azure;

namespace BWJ.Core.CosmosRepository
{
    public interface IBusinessEntity
    {
        DateTimeOffset? Timestamp { get; set; }
        ETag ETag { get; set; }
    }
}
