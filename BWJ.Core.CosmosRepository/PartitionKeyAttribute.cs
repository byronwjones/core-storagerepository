namespace BWJ.Core.CosmosRepository
{
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class PartitionKeyAttribute : Attribute { }
}
