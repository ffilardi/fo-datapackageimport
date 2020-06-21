using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos.Table;

namespace FO_DataPackageImport
{
    public static class PackageStatusUpdate
    {
        [FunctionName("PackageStatusUpdate")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string accountName = Environment.GetEnvironmentVariable("StorageAccountName");
            string accountKey = Environment.GetEnvironmentVariable("StorageAccountKey");
            
            EventEntity newEntity;

            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                
                if (String.IsNullOrEmpty(requestBody))
                {
                    throw new Exception("Empty request body");
                }

                BusinessEvent businessEvent = JsonConvert.DeserializeObject<BusinessEvent>(requestBody);

                StorageCredentials creds = new StorageCredentials(accountName, accountKey);
                CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);

                CloudTableClient client = account.CreateCloudTableClient();
                CloudTable table = client.GetTableReference(Environment.GetEnvironmentVariable("StorageTable"));
                
                table.CreateIfNotExists();

                newEntity = new EventEntity()
                {
                    PartitionKey = businessEvent.EntityName,
                    RowKey = businessEvent.EventId,
                    BusinessEventId = businessEvent.BusinessEventId,
                    ProjectName = businessEvent.ProjectName,
                    ProjectDescription = businessEvent.ProjectDescription,
                    ExecutionId = businessEvent.ExecutionId,
                    LegalEntity = businessEvent.LegalEntity,
                    NoOfRecords = businessEvent.NoOfRecords,
                    NoOfCreatedRecords = businessEvent.NoOfCreatedRecords,
                    NoOfUpdatedRecords = businessEvent.NoOfUpdatedRecords,
                    NoOfErrorRecords = businessEvent.NoOfErrorRecords,
                    OperationType = businessEvent.OperationType,
                    ProjectCategory = businessEvent.ProjectCategory,
                    StartedDateTime = businessEvent.StartedDateTime,
                    EndDateTime = businessEvent.EndDateTime,
                    Status = businessEvent.Status
                };

                await table.ExecuteAsync(TableOperation.Insert(newEntity));
            }
            catch (Exception ex)
            {
                log.LogError($"Error: '{ex.Message}'");
                return new BadRequestObjectResult(ex.Message);
            }

            return new OkObjectResult(newEntity);
        }
    }

    public class EventEntity : TableEntity
    {
        public EventEntity(string entityName, string eventId)
        {
            this.PartitionKey = entityName;
            this.RowKey = eventId;
        }

        public EventEntity() { }

        public string BusinessEventId { get; set; }

        public string ProjectName { get; set; }

        public string ProjectDescription { get; set; }

        public string ExecutionId { get; set; }

        public string LegalEntity { get; set; }

        public long NoOfRecords { get; set; }

        public long NoOfCreatedRecords { get; set; }

        public long NoOfUpdatedRecords { get; set; }

        public long NoOfErrorRecords { get; set; }

        public string OperationType { get; set; }

        public string ProjectCategory { get; set; }

        public DateTimeOffset StartedDateTime { get; set; }

        public DateTimeOffset EndDateTime { get; set; }

        public string Status { get; set; }
    }

    public partial class BusinessEvent
    {
        [JsonProperty("BusinessEventId")]
        public string BusinessEventId { get; set; }

        [JsonProperty("ControlNumber")]
        public long ControlNumber { get; set; }

        [JsonProperty("EndDateTime")]
        public DateTimeOffset EndDateTime { get; set; }

        [JsonProperty("EntityName")]
        public string EntityName { get; set; }

        [JsonProperty("EventId")]
        public string EventId { get; set; }

        [JsonProperty("EventTime")]
        public string EventTime { get; set; }

        [JsonProperty("ExecutionId")]
        public string ExecutionId { get; set; }

        [JsonProperty("ProjectName")]
        public string ProjectName { get; set; }

        [JsonProperty("ProjectDescription")]
        public string ProjectDescription { get; set; }

        [JsonProperty("LegalEntity")]
        public string LegalEntity { get; set; }

        [JsonProperty("MajorVersion")]
        public long MajorVersion { get; set; }

        [JsonProperty("MinorVersion")]
        public long MinorVersion { get; set; }

        [JsonProperty("NoOfCreatedRecords")]
        public long NoOfCreatedRecords { get; set; }

        [JsonProperty("NoOfRecords")]
        public long NoOfRecords { get; set; }

        [JsonProperty("NoOfUpdatedRecords")]
        public long NoOfUpdatedRecords { get; set; }

        [JsonProperty("NoOfErrorRecords")]
        public long NoOfErrorRecords { get; set; }

        [JsonProperty("OperationType")]
        public string OperationType { get; set; }

        [JsonProperty("ProjectCategory")]
        public string ProjectCategory { get; set; }

        [JsonProperty("RecId")]
        public long RecId { get; set; }

        [JsonProperty("StartedDateTime")]
        public DateTimeOffset StartedDateTime { get; set; }

        [JsonProperty("Status")]
        public string Status { get; set; }
    }
}
