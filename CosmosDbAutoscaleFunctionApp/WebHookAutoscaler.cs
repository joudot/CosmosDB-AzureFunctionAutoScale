using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Linq;

namespace CosmosDbAutoscaleFunctionApp
{
    public static class WebHookAutoscaler
    {
        private static readonly IDocumentClient _client = new DocumentClient(
                new Uri(Environment.GetEnvironmentVariable("EndpointUrl")),
                Environment.GetEnvironmentVariable("AuthorizationKey"));

        private static readonly Uri _collectionUri = UriFactory.CreateDocumentCollectionUri(
                Environment.GetEnvironmentVariable("DatabaseName"),
                Environment.GetEnvironmentVariable("CollectionName"));

        private static readonly int _maxAuthorizedRu = int.Parse(Environment.GetEnvironmentVariable("MaxAuthorizedRu"));

        private static readonly int _minAuthorizedRu = int.Parse(Environment.GetEnvironmentVariable("MinAuthorizedRu"));

        private static readonly string _scaleUp = "Up";

        private static readonly string _scaleDown = "Down";

        [FunctionName("WebHookAutoscaler")]
        public static async System.Threading.Tasks.Task<IActionResult> RunAsync([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequest req, ILogger log)
        {
            try
            {
                log.LogInformation("C# HTTP trigger function processed a request.");

                string action = req.Query["action"];

                if (string.IsNullOrEmpty(action)
                    || (!action.Equals(_scaleUp, StringComparison.InvariantCultureIgnoreCase)
                    && !action.Equals(_scaleDown, StringComparison.InvariantCultureIgnoreCase)))
                {
                    return new BadRequestObjectResult($"Please pass an action on the query string: {_scaleUp} or {_scaleDown}");
                }

                var collection = await _client.ReadDocumentCollectionAsync(_collectionUri);

                var offer = (OfferV2) _client.CreateOfferQuery()
                    .Where(r => r.ResourceLink == collection.Resource.SelfLink)
                    .AsEnumerable()
                    .SingleOrDefault();
                int newThroughput = 0;
                var currentThroughput = offer.Content.OfferThroughput;

                if (action.Equals(_scaleUp, StringComparison.InvariantCultureIgnoreCase))
                {
                    // Note: trigger this operation from the Alerts view in Cosmos DB, based on the number of throttle requests
                    if (currentThroughput * 2 <= _maxAuthorizedRu)
                    {
                        newThroughput = currentThroughput * 2;
                    }
                    else if (currentThroughput + 1 <= _maxAuthorizedRu)
                    {
                        newThroughput = currentThroughput + 1;
                    }
                    else
                    {
                        return new OkObjectResult("Max Throughput reached");
                    }
                }
                else if(action.Equals(_scaleDown, StringComparison.InvariantCultureIgnoreCase))
                {
                    // Note: to trigger this operation from the Alerts view in Cosmos DB, based on the number of throttle requests
                    if (currentThroughput / 2 >= _minAuthorizedRu)
                    {
                        newThroughput = currentThroughput / 2;
                    }
                    else if (currentThroughput - 1 >= _minAuthorizedRu)
                    {
                        newThroughput = currentThroughput - 1;
                    }
                    else
                    {
                        return new OkObjectResult("Min Throughput reached");
                    }
                }

                offer = new OfferV2(offer, newThroughput);
                await _client.ReplaceOfferAsync(offer);

                return new OkObjectResult($"Hello, {action}");
            }
            catch (Exception e)
            {
                return new BadRequestObjectResult($"{e.Message}");
            }
        }
    }
}
