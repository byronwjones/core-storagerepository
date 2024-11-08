using Azure.Data.Tables;

namespace BWJ.Core.CosmosRepository
{
    internal class CachedTableClient
    {
        public CachedTableClient(
            string tableName,
            CosmosRepositoryConfiguration config,
            DateTime lastAccessed)
        {
            TableClient = new TableClient(
                            new Uri(config.StorageUri),
                            tableName,
                            new TableSharedKeyCredential(config.AccountName, config.AccountKey));
            TableClient.CreateIfNotExists();
            LastAccessed = lastAccessed;
        }

        public TableClient TableClient { get; private set; }
        public DateTime LastAccessed { get; set; }
    }
}
