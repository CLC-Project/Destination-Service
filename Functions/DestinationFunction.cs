using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using System;
using DestinationService.Dtos;
using DestinationService.Events;
using MongoDB.Bson;
using DestinationService.Entities;
using System.Linq;

namespace DestinationService.Functions
{
    public static class DestinationFunction
    {
        private const string _databaseName = "CLC-Project";
        private const string _collectionName = "destinations";
        private const string _dbConnection = "MongoDBAtlasConnection";
        private const string _serviceBusConnection = "ServiceBusConnection";
        private const string _queueName = "destination_crud_events";

        #region Util

        private static IMongoCollection<Destination> GetCollection()
        {
            var settings = MongoClientSettings.FromConnectionString(Environment.GetEnvironmentVariable(_dbConnection));
            settings.ServerApi = new ServerApi(ServerApiVersion.V1);
            var client = new MongoClient(settings);
            var database = client.GetDatabase(_databaseName);
            var collection = database.GetCollection<Destination>(_collectionName);
            return collection;
        }

        #endregion

        #region Get

        [FunctionName("Destinations_GetById")]
        public static async Task<IActionResult> GetById([HttpTrigger(AuthorizationLevel.Function, "get", Route = "destinations/{id}")] HttpRequest req, 
                                                         string id,
                                                         ILogger log)
        {
            log.LogInformation($"{DateTime.Now}: get destination by id");

            var collection = GetCollection();
            var entry = await collection.FindAsync(x => x.Id == ObjectId.Parse(id));
            var result = await entry.FirstOrDefaultAsync();

            return new OkObjectResult(result is null ? result : new DestinationDto(result));

        }


        [FunctionName("Destinations_GetByUserId")]
        public static async Task<IActionResult> GetByUserId([HttpTrigger(AuthorizationLevel.Function, "get", Route = "destinations/user/{userId}")] HttpRequest req,
                                                             string userId,
                                                             ILogger log)
        {
            log.LogInformation($"{DateTime.Now}: get destination by user id");

            var collection = GetCollection();
            var entries = await collection.FindAsync(x => x.User == userId);
            var result = await entries.ToListAsync();

            return new OkObjectResult(result.Select(x => new DestinationDto(x)));
        }

        #endregion

        #region Post

        [FunctionName("Destinations_Post")]
        [return: ServiceBus(_queueName, Connection = _serviceBusConnection)]
        public static async Task<DestinationCrudEvent> Post([HttpTrigger(AuthorizationLevel.Function, "post", Route = "destinations")] DestinationDto dto, 
                                                             ILogger log)
        {
            log.LogInformation($"{DateTime.Now}: insert new destination");

            var destination = new Destination
            {
                Country = dto.Country,
                Region = dto.Region,
                City = dto.City,
                User = dto.User
            };
            var collection = GetCollection();
            await collection.InsertOneAsync(destination);

            return new DestinationCrudEvent { Type = DestinationCrudEvent.EventType.Insert, DestinationId = destination.Id.ToString() };
        }

        #endregion

        #region Delete

        [FunctionName("Destinations_Delete")]
        [return: ServiceBus(_queueName, Connection = _serviceBusConnection)]
        public static async Task<DestinationCrudEvent> Delete([HttpTrigger(AuthorizationLevel.Function, "delete", Route = "destinations/{id}")] HttpRequest req,
                                                        string id,
                                                        ILogger log)
        {
            log.LogInformation($"{DateTime.Now}: delete destination by id");

            var collection = GetCollection();
            await collection.DeleteOneAsync(x => x.Id == ObjectId.Parse(id));

            return new DestinationCrudEvent { Type = DestinationCrudEvent.EventType.Delete, DestinationId = id };
        }

        #endregion
    }
}
