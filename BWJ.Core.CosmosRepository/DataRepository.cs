using Azure;
using Azure.Data.Tables;
using BWJ.Core.Chronology;
using System.Linq.Expressions;
using System.Reflection;

namespace BWJ.Core.CosmosRepository
{
    public abstract class DataRepository<TBusinessEntity, TStorageEntity>
        where TBusinessEntity : class, IBusinessEntity
        where TStorageEntity : class, IStorageEntity
    {
        public DataRepository(
            CosmosRepositoryConfiguration connectionInfo,
            IDateTimeService dateTime)
        {
            AssertBusinessEntityValid();
            AssertStorageEntityValid();
            _KeyBusinessEntityPropertiesAssignor = BuildKeyBusinessEntityPropertiesAssignor();
            _KeyStorageEntityPropertiesAssignor = BuildKeyStorageEntityPropertiesAssignor();

            _config = connectionInfo;
            _dateTime = dateTime;

            _lastCachePurge = dateTime.GetCurrentTimeUtc();
        }

        private readonly Action<TBusinessEntity, TStorageEntity> _KeyBusinessEntityPropertiesAssignor;
        private readonly Action<TBusinessEntity, TStorageEntity> _KeyStorageEntityPropertiesAssignor;

        protected abstract string TableNameTemplate { get; }

        protected abstract TBusinessEntity ToBusinessEntity(TStorageEntity storageEntity);
        protected abstract TStorageEntity ToStorageEntity(TBusinessEntity businessEntity);

        protected async Task<IEnumerable<TBusinessEntity>> GetManyById(string id, params string[] tableNameParameters)
            => await GetMany(x => x.RowKey == id, tableNameParameters);

        protected async Task<IEnumerable<TBusinessEntity>> GetManyByIds(IEnumerable<string> ids, params string[] tableNameParameters)
        {
            var result = new List<TBusinessEntity>();
            foreach (var id in ids)
            {
                var entities = await GetMany(x => x.RowKey == id, tableNameParameters);
                result.AddRange(entities);
            }

            return result.ToArray();
        }

        protected async Task<TBusinessEntity?> GetOneById(string id, params string[] tableNameParameters)
            => await GetOne(x => x.RowKey == id, tableNameParameters);

        protected async Task<IEnumerable<TBusinessEntity>> GetByPartition(string partitionKey, params string[] tableNameParameters)
            => await GetMany(x => x.PartitionKey == partitionKey, tableNameParameters);

        protected async Task<IEnumerable<TBusinessEntity>> GetAll(params string[] tableNameParameters)
            => await GetMany(x => true, tableNameParameters);

        /// <summary>
        /// Returns all entities where the given property has a value contained in the given inclusion collection
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="propertyLambda"></param>
        /// <param name="includeValues"></param>
        /// <param name="tableNameParameters"></param>
        /// <returns></returns>
        protected async Task<IEnumerable<TBusinessEntity>> GetContains<TValue>(
            Expression<Func<TStorageEntity, TValue>> propertyLambda,
            IEnumerable<TValue> includeValues,
            params string[] tableNameParameters)
        {
            var result = new List<TBusinessEntity>();
            var propertyName = GetPropertyName(propertyLambda);

            foreach (var value in includeValues)
            {
                var filter = GetODataEqualityFilter(propertyName, value);
                var entities = await GetMany(filter, tableNameParameters);
                result.AddRange(entities);
            }

            return result.ToArray();
        }

        protected async Task<TBusinessEntity?> GetEntity(string id, string partitionKey, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            var result = await client.GetEntityIfExistsAsync<TStorageEntity>(partitionKey, id);
            return result.HasValue ? ConvertToBusinessEntity(result.Value) : null;
        }

        protected async Task<IEnumerable<TBusinessEntity>> GetMany(Expression<Func<TStorageEntity, bool>> filter, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            var entities = client.QueryAsync(filter);
            var result = new List<TBusinessEntity>();
            await foreach (var entity in entities)
            {
                result.Add(ConvertToBusinessEntity(entity));
            }

            return result;
        }

        protected async Task<IEnumerable<TBusinessEntity>> GetMany(string filter, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            var entities = client.QueryAsync<TStorageEntity>(filter);
            var result = new List<TBusinessEntity>();
            await foreach (var entity in entities)
            {
                result.Add(ConvertToBusinessEntity(entity));
            }

            return result;
        }

        protected async Task<TBusinessEntity?> GetOne(Expression<Func<TStorageEntity, bool>> filter, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            var entities = client.QueryAsync(filter);
            await foreach (var entity in entities)
            {
                return ConvertToBusinessEntity(entity);
            }

            return null;
        }

        protected async Task CreateEntity(TBusinessEntity entity, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            var storageEntity = ConvertToStorageEntity(entity);
            await client.AddEntityAsync(storageEntity);
        }

        protected async Task UpdateEntity(TBusinessEntity entity, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            var storageEntity = ConvertToStorageEntity(entity);
            if(entity.__OriginalPartitionKey is not null && storageEntity.PartitionKey != entity.__OriginalPartitionKey)
            {
                await client.DeleteEntityAsync(entity.__OriginalPartitionKey, storageEntity.RowKey);
                await client.AddEntityAsync(storageEntity);
            }
            else
            {
                await client.UpdateEntityAsync(storageEntity, storageEntity.ETag, TableUpdateMode.Replace);
            }
        }

        /// <summary>
        /// WARNING: This method does not protect against concurrency conflict overwrites.
        /// Always prefer usage of CreateEntity and UpdateEntity, unless there is a REALLY good business case for using this
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        protected async Task UpsertEntity(TBusinessEntity entity, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            var storageEntity = ConvertToStorageEntity(entity);
            if (entity.__OriginalPartitionKey is not null && storageEntity.PartitionKey != entity.__OriginalPartitionKey)
            {
                await client.DeleteEntityAsync(entity.__OriginalPartitionKey, storageEntity.RowKey);
            }
            await client.UpsertEntityAsync(storageEntity, TableUpdateMode.Replace);
        }

        protected async Task DeleteEntity(string id, string partitionKey, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            await client.DeleteEntityAsync(partitionKey, id);
        }

        protected async Task DeleteEntity(TBusinessEntity entity, params string[] tableNameParameters)
        {
            var client = GetTableClient(tableNameParameters);
            var storageEntity = ConvertToStorageEntity(entity);
            await client.DeleteEntityAsync(storageEntity.PartitionKey, storageEntity.RowKey);
        }

        private string GetODataEqualityFilter(string propertyName, object? evaluatedValue)
        {
            var valueExpression = string.Empty;
            if (evaluatedValue is string)
            {
                valueExpression = $"'{evaluatedValue}'";
            }
            else if (evaluatedValue is DateTime)
            {
                var date = (DateTime)evaluatedValue;
                valueExpression = date.ToString("yyyy-MM-ddTHH:mm:ssZ");
            }
            else
            {
                valueExpression = evaluatedValue?.ToString() ?? "null";
            }

            return $"{propertyName} eq {valueExpression}";
        }

        private string GetPropertyName<TProperty>(Expression<Func<TStorageEntity, TProperty>> propertyLambda)
        {
            UnaryExpression? unary = propertyLambda.Body as UnaryExpression;
            MemberExpression? member = (unary is not null) ?
                unary.Operand as MemberExpression : propertyLambda.Body as MemberExpression;

            if (member == null)
            {
                throw new ArgumentException($"Expression '{propertyLambda}' refers to a method, not a property.");
            }

            PropertyInfo? property = member.Member as PropertyInfo;
            if (property == null)
            {
                throw new ArgumentException($"Expression '{propertyLambda}' refers to a field, not a property.");
            }

            return property.Name;
        }

        private void AssertBusinessEntityValid()
        {
            var t = typeof(TBusinessEntity);
            var rowKeys = t.GetProperties()
                .Where(p => p.GetCustomAttribute<RowKeyAttribute>() is not null && p.Name != "RowKey")
                .ToArray();
            var rowKeyProp = t.GetProperty("RowKey");

            var partitionKeys = t.GetProperties()
                .Where(p => p.GetCustomAttribute<PartitionKeyAttribute>() is not null && p.Name != "PartitionKey")
                .ToArray();
            var partitionKeyProp = t.GetProperty("PartitionKey");

            if (rowKeys.Length > 1) {
                throw new RepositoryDataConfigurationException($"Business entity {t.FullName} has multiple properties decorated with the RowKey attribute");
            }
            if(rowKeys.Any() && rowKeyProp != null)
            {
                throw new RepositoryDataConfigurationException($"Ambiguous configuration: Business entity {t.FullName} has a property named RowKey and a property marked with the RowKey attribute");
            }
            if (partitionKeys.Any(p => p.Name == "RowKey"))
            {
                throw new RepositoryDataConfigurationException($"Tomfoolery: Business entity {t.FullName} has a property named RowKey, decorated with a PartitionKey attribute");
            }
            if(rowKeyProp is not null && rowKeyProp.PropertyType != typeof(string))
            {
                throw new RepositoryDataConfigurationException($"Property {t.FullName}.RowKey must be of type string");
            }
            var rk = rowKeys.FirstOrDefault();
            if (rk is not null && rk.PropertyType != typeof(string))
            {
                throw new RepositoryDataConfigurationException($"Property {t.FullName}.{rk.Name} is marked as the RowKey, thus must be of type string");
            }

            if (partitionKeys.Length > 1)
            {
                throw new RepositoryDataConfigurationException($"Business entity {t.FullName} has multiple properties decorated with the PartitionKey attribute");
            }
            if (partitionKeys.Any() && partitionKeyProp != null)
            {
                throw new RepositoryDataConfigurationException($"Ambiguous configuration: Business entity {t.FullName} has a property named PartitionKey and a property marked with the PartitionKey attribute");
            }
            if (rowKeys.Any(p => p.Name == "PartitionKey"))
            {
                throw new RepositoryDataConfigurationException($"Tomfoolery: Business entity {t.FullName} has a property named PartitionKey, decorated with a RowKey attribute");
            }
            if (partitionKeyProp is not null && partitionKeyProp.PropertyType != typeof(string))
            {
                throw new RepositoryDataConfigurationException($"Property {t.FullName}.PartitionKey must be of type string");
            }
            var pk = partitionKeys.FirstOrDefault();
            if (pk is not null && pk.PropertyType != typeof(string))
            {
                throw new RepositoryDataConfigurationException($"Property {t.FullName}.{pk.Name} is marked as the PartitionKey, thus must be of type string");
            }
        }
        private void AssertStorageEntityValid()
        {
            var validPropertyTypes = new Type[] {
                typeof(byte[]),
                typeof(bool),
                typeof(DateTime),
                typeof(double),
                typeof(Guid),
                typeof(int),
                typeof(long),
                typeof(string),
                typeof(DateTimeOffset),
                typeof(ETag),
            };

            var t = typeof(TStorageEntity);
            var invalidProperty = t.GetProperties()
                .FirstOrDefault(p => IsValidStorageEntityType(p.PropertyType) == false);
            if (invalidProperty != null)
            {
                throw new RepositoryDataConfigurationException($"Storage entity property {t.FullName}.{invalidProperty.Name} is invalid: Type not supported by Azure Table Storage / Cosmos DB.  Valid types can be found here: https://learn.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model?redirectedfrom=MSDN#property-types");
            }
        }

        private static bool IsValidStorageEntityType(Type type)
        {
            var validPropertyTypes = new Type[] {
                typeof(byte[]),
                typeof(bool),
                typeof(bool?),
                typeof(DateTime),
                typeof(double),
                typeof(Guid),
                typeof(int),
                typeof(long),
                typeof(string),
                typeof(DateTimeOffset),
                typeof(ETag),
            };

            if (validPropertyTypes.Contains(type)) { return true; }
            if (type.IsGenericType && type.Name.Contains("Nullable"))
            {
                var nullableType = type.GetGenericArguments()[0];
                if (validPropertyTypes.Contains(nullableType)) { return true; }
            }

            return false;
        }

        private PropertyInfo GetRowKeyProperty(Type t)
        {
            var rowKey = t.GetProperties()
                .FirstOrDefault(p => p.GetCustomAttribute<RowKeyAttribute>() is not null);
            if(rowKey is null)
            {
                rowKey = t.GetProperty("RowKey");
            }
            return rowKey!;
        }
        private PropertyInfo GetPartitionKeyProperty(Type t)
        {
            var pKey = t.GetProperties()
                .FirstOrDefault(p => p.GetCustomAttribute<PartitionKeyAttribute>() is not null);
            if (pKey is null)
            {
                pKey = t.GetProperty("PartitionKey");
            }
            return pKey!;
        }

        public Action<TBusinessEntity, TStorageEntity> BuildKeyBusinessEntityPropertiesAssignor()
        {
            var tb = typeof(TBusinessEntity);
            var ts = typeof(TStorageEntity);

            var storageRowProp = ts.GetProperty("RowKey")!;
            var businessRowProp = GetRowKeyProperty(tb);

            var storagePartitionProp = ts.GetProperty("PartitionKey")!;
            var businessPartitionProp = GetPartitionKeyProperty(tb);
            var originalPartitionProp = tb.GetProperty("__OriginalPartitionKey")!;

            var businessParam = Expression.Parameter(tb, "businessEntity");
            var storageParam = Expression.Parameter(ts, "storageEntity");

            var storageRowExp = Expression.Property(storageParam, storageRowProp);
            var businessRowExp = Expression.Property(businessParam, businessRowProp);
            var originalPartitionExp = Expression.Property(businessParam, originalPartitionProp);

            var storagePartitionExp = Expression.Property(storageParam, storagePartitionProp);
            var businessPartitionExp = Expression.Property(businessParam, businessPartitionProp);

            var rowAssign = Expression.Assign(businessRowExp, storageRowExp);
            var partAssign = Expression.Assign(businessPartitionExp, storagePartitionExp);
            var origAssign = Expression.Assign(originalPartitionExp, storagePartitionExp);
            var body = Expression.Block(rowAssign, partAssign, origAssign);

            var action = Expression.Lambda<Action<TBusinessEntity, TStorageEntity>>(body, businessParam, storageParam).Compile();
            return action;
        }
        public Action<TBusinessEntity, TStorageEntity> BuildKeyStorageEntityPropertiesAssignor()
        {
            var tb = typeof(TBusinessEntity);
            var ts = typeof(TStorageEntity);

            var storageRowProp = ts.GetProperty("RowKey")!;
            var businessRowProp = GetRowKeyProperty(tb);

            var storagePartitionProp = ts.GetProperty("PartitionKey")!;
            var businessPartitionProp = GetPartitionKeyProperty(tb);

            var businessParam = Expression.Parameter(tb, "businessEntity");
            var storageParam = Expression.Parameter(ts, "storageEntity");

            var storageRowExp = Expression.Property(storageParam, storageRowProp);
            var businessRowExp = Expression.Property(businessParam, businessRowProp);

            var storagePartitionExp = Expression.Property(storageParam, storagePartitionProp);
            var businessPartitionExp = Expression.Property(businessParam, businessPartitionProp);

            var rowAssign = Expression.Assign(storageRowExp, businessRowExp);
            var partAssign = Expression.Assign(storagePartitionExp, businessPartitionExp);
            var body = Expression.Block(rowAssign, partAssign);

            var action = Expression.Lambda<Action<TBusinessEntity, TStorageEntity>>(body, businessParam, storageParam).Compile();
            return action;
        }

        private TBusinessEntity ConvertToBusinessEntity(TStorageEntity storageEntity)
        {
            var b = ToBusinessEntity(storageEntity);
            _KeyBusinessEntityPropertiesAssignor(b, storageEntity);

            return b;
        }

        private TStorageEntity ConvertToStorageEntity(TBusinessEntity businessEntity)
        {
            var s = ToStorageEntity(businessEntity);
            _KeyStorageEntityPropertiesAssignor(businessEntity, s);

            return s;
        }

        private TableClient GetTableClient(params string[] tableNameParameters)
        {
            var tableName = GetTableName(tableNameParameters);
            TableClient? tableClient = null;
            lock (@lock)
            {
                tableClient = FetchTableClient(tableName);
                CleanUpTableCache();
            }

            return tableClient;
        }

        private TableClient FetchTableClient(string tableName)
        {
            var lastAccess = _dateTime.GetCurrentTimeUtc();
            if (_clients.ContainsKey(tableName))
            {
                var client = _clients[tableName];
                client.LastAccessed = lastAccess;
                return client.TableClient;
            }
            else
            {
                var client = new CachedTableClient(tableName, _config, lastAccess);
                _clients.Add(tableName, client);
                return client.TableClient;
            }
        }
        private void CleanUpTableCache()
        {
            var purgeTime = _lastCachePurge.AddMinutes(_config.PurgeCachedClientsEvery_Minutes);
            var now = _dateTime.GetCurrentTimeUtc();
            if (purgeTime > now) { return; } // not time to purge yet

            var minLastAccessedTime = now.AddMinutes(_config.TableClientCacheDuration * -1);
            var purgableTables = _clients.Where(x => x.Value.LastAccessed < minLastAccessedTime)
                .Select(x => x.Key);
            foreach (var table in purgableTables)
            {
                _clients.Remove(table);
            }
            _lastCachePurge = now;
        }

        private string GetTableName(params string[] tableNameParameters)
        {
            if (tableNameParameters.Any())
            {
                var tableName = string.Format(TableNameTemplate, tableNameParameters);
                return tableName;
            }
            else
            {
                return TableNameTemplate;
            }
        }

        private readonly CosmosRepositoryConfiguration _config;
        private readonly IDateTimeService _dateTime;

        private object @lock = new object();
        private Dictionary<string, CachedTableClient> _clients = new Dictionary<string, CachedTableClient>();
        private DateTime _lastCachePurge;
    }
}
