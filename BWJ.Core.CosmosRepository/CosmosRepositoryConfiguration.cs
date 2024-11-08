namespace BWJ.Core.CosmosRepository
{
    public class CosmosRepositoryConfiguration
    {
        public string StorageUri
        {
            get => $"https://{AccountName}.table.{(DataService == DocumentDatabaseService.StorageTables ? "core.windows.net" : "cosmosdb.azure.com")}";
            set { }
        }
        public DocumentDatabaseService DataService { get; set; }
        public string AccountName { get; set; } = string.Empty;
        public string AccountKey { get; set; } = string.Empty;
        public int PurgeCachedClientsEvery_Minutes { get; set; } = 60;
        public int TableClientCacheDuration { get; set; } = 120;
    }
}
