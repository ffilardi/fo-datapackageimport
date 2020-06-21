using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using RestSharp;

namespace FO_DataPackageImport
{
    public static class PackageImport
    {
        [FunctionName("PackageImport")]
        public static async Task Run([BlobTrigger("package-import/{name}.zip", Connection = "AzureWebJobsStorage")]Stream blob, string name, ILogger log)
        {
            try
            {
                var token = GetToken(
                    Environment.GetEnvironmentVariable("ClientId"),
                    Environment.GetEnvironmentVariable("ClientSecret"),
                    Environment.GetEnvironmentVariable("Tenant"),
                    Environment.GetEnvironmentVariable("Resource"));

                if (String.IsNullOrEmpty(token))
                    throw new Exception($"Unable to get a token, package '{name}' aborted");

                var destinationUrl = await UploadBlob(
                    Environment.GetEnvironmentVariable("Resource"),
                    blob, name, token);

                if (String.IsNullOrEmpty(destinationUrl))
                    throw new Exception($"Unable to get a writable destination URL, package '{name}' aborted");

                var executionId = ExecuteImport(
                    Environment.GetEnvironmentVariable("Resource"),
                    Environment.GetEnvironmentVariable("LegalEntity"),
                    Environment.GetEnvironmentVariable("ProjectName"),
                    destinationUrl, token);

                if (String.IsNullOrEmpty(executionId))
                    throw new Exception($"Package '{name}' coudln't be loaded");

                log.LogInformation($"***** Data package '{name}' loaded (Execution Id '{executionId}')");
            }
            catch (Exception ex)
            {
                log.LogError($"Error: '{ex.Message}'");
            }
        }

        public static string GetToken(string clientId, string clientSecret, string tenant, string resource)
        {
            var client = new RestClient($"https://login.microsoftonline.com/{tenant}/oauth2/token");
            var request = new RestRequest(Method.POST);

            request.AddHeader("Content-Type", "application/x-www-form-urlencoded");
            request.AddParameter("grant_type", "client_credentials");
            request.AddParameter("client_id", clientId);
            request.AddParameter("client_secret", clientSecret);
            request.AddParameter("resource", resource);

            IRestResponse response = client.Execute(request);

            JObject getTokenResponse = JObject.Parse(response.Content);
            
            return (string)getTokenResponse["access_token"];
        }

        public static async Task<string> UploadBlob(string resource, Stream blob, string filename, string token)
        {
            // Get writable URL
            var client = new RestClient($"{resource}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetAzureWriteUrl");

            JObject requestBody = new JObject();
            requestBody.Add("uniqueFileName", filename);

            var request = new RestRequest(Method.POST);

            request.AddHeader("Authorization", $"Bearer {token}");
            request.AddHeader("Content-Type", "application/json");
            request.AddParameter("application/json,text/plain", requestBody, ParameterType.RequestBody);

            IRestResponse response = client.Execute(request);

            JObject getUrlResponse = JObject.Parse(response.Content);
            JObject getUrlResponseValue = JObject.Parse((string)getUrlResponse["value"]);

            var destinationUrl = (string)getUrlResponseValue["BlobUrl"];

            // Upload blob
            CloudBlockBlob cloudBlockBlob = new CloudBlockBlob(new Uri(destinationUrl));
            await cloudBlockBlob.UploadFromStreamAsync(blob);

            return destinationUrl;
        }

        public static string ExecuteImport(string resource, string legalEntity, string projectName, string destinationUrl, string token)
        {
            var client = new RestClient($"{resource}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.ImportFromPackage");

            JObject requestBody = new JObject();

            requestBody.Add("packageUrl", destinationUrl);
            requestBody.Add("definitionGroupId", projectName);
            requestBody.Add("executionId", "");
            requestBody.Add("execute", true);
            requestBody.Add("overwrite", true);
            requestBody.Add("legalEntityId", legalEntity);

            var request = new RestRequest(Method.POST);

            request.AddHeader("Authorization", $"Bearer {token}");
            request.AddHeader("Content-Type", "application/json");
            request.AddParameter("application/json,text/plain", requestBody, ParameterType.RequestBody);

            IRestResponse response = client.Execute(request);

            JObject callImportResponse = JObject.Parse(response.Content);

            return (string)callImportResponse["value"];
        }
    }
}
