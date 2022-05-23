using Linklives.Domain;
using log4net;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Linklives.Indexer.Utils
{
    public class ESHelper
    {
        private readonly Nest.ElasticClient _esClient;
        private static readonly ILog Log = LogManager.GetLogger(System.Reflection.MethodInfo.GetCurrentMethod().DeclaringType.Name);

        public ESHelper(ElasticClient esClient)
        {
            _esClient = esClient;
        }
        public string CreateNewIndex<T>(string index, bool dateDetection = false) where T : class
        {
            var date = DateTime.Now.ToString("dd-MM-yyyy_hh-mm-ss");
            var indexname = $"{index}_{date}";
            _esClient.Indices.Create(indexname, c => c
                .Map<T>(m => m
                    .DateDetection(dateDetection)
                    .AutoMap())
                .Settings(s => s
                    .Setting(UpdatableIndexSettings.MaxResultWindow, 1000))
            );

            return indexname;
        }
        public void ActivateNewIndices(IDictionary<string, string> indexMappings)
        {
            foreach (var indexMap in indexMappings)
            {
                ActivateNewIndex(indexMap.Key, indexMap.Value);
            }
        }
        public void ActivateNewIndex(string indexAlias, string indexName)
        {
            //Cleanup existing aliases
            Log.Debug($"Deleting existing aliases for {indexAlias}");
            _esClient.Indices.DeleteAlias($"{indexAlias}*", "_all");
            //Add our new alias
            Log.Debug($"Puting new alias: {indexAlias} for index: {indexName}");
            _esClient.Indices.PutAlias(indexName, indexAlias);
            //Find and delete all the old versions of this index
            Log.Debug($"Deleting old instances of index: {indexAlias}");
            var oldIndices = _esClient.Indices.Get($"{indexAlias}*").Indices.Where(i => i.Key.Name != indexName);
            foreach (var oldIndex in oldIndices)
            {
                RemoveIndex(oldIndex.Key);
            }
        }
        public void RemoveIndex(IndexName index)
        {
            Log.Debug($"Deleting {index}");
            _esClient.Indices.Delete(index);
        }
        public void BulkIndexDocs<T>(IEnumerable<T> docs, string index) where T : class
        {
            //TODO: Fetch some of these config values from enviroment config instead of hardcording.
            //TODO: Maybe pass in a proper oberserver so we can handle events other than onNext and leave handling of events to the calling code instead of doing it here. see: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/indexing-documents.html#_advanced_bulk_indexing
            var bulkAllObservable = _esClient.BulkAll(docs, b => b
               .Index(index)
               .BackOffTime("30s")
               .BackOffRetries(3)
               .RefreshOnCompleted()
               .MaxDegreeOfParallelism(3)
               .Size(3000)
               .Timeout(TimeSpan.FromMinutes(1))
               .DroppedDocumentCallback((item, Document) =>
               {
                   Log.Debug($"The document {item} can not be indexed. Bulk all indexing will be halted.");
               }))
               .Wait(TimeSpan.FromHours(24),
                    onNext: response => { Log.Debug($"Page: {response.Page} containing: {response.Items.Count} items sucessfully indexed to {index}");}
               );              
        }

        public void IndexManyDocs<T>(IEnumerable<T> docs, string index) where T : class
        {
            Log.Debug($"Indexing documents in index {index}");
            try
            {
                var bulkIndexPAsResponse = _esClient.Bulk(b => b
                                                                .Index(index)
                                                                .Timeout(TimeSpan.FromMinutes(7))
                                                                .IndexMany(docs)
                                                            );
                if (bulkIndexPAsResponse.Errors || !bulkIndexPAsResponse.ApiCall.Success)
                {
                    Log.Warn("Could not index documents in bulk indexation:" + bulkIndexPAsResponse.ApiCall.OriginalException);
                }
            }
            catch(Exception e)
            {
                Log.Warn("Could not index documents in bulk indexation: " + e.Message);
            }   
        }

        /// <summary>
        /// Updates lifecourses with PAs and sets sortable parameters for the lifecourse
        /// </summary>
        /// <param name="updates">A list of Tuple<string,BasePA> that holdes lifecourse and the item to update it with</param>
        /// <param name="index"></param>
        public void UpdateMany<T>(string updateScript, List<Tuple<string, T>> updates, string index)
        {
            Log.Debug($"Updating {updates.Count} documents in index {index}");

            try
            {
                var bulkUpdateLifecoursesResponse = _esClient.Bulk(b => b
                                .Index(index)
                                .Timeout(TimeSpan.FromMinutes(10))
                                .UpdateMany(updates, (descriptor, update) => descriptor
                                    .Id(update.Item1)
                                    .Script(s => s
                                        .Source(updateScript)
                                        .Params(p => p
                                            .Add("pa", update.Item2)
                                        )
                                    )
                                )
                            );

                if (bulkUpdateLifecoursesResponse.Errors || !bulkUpdateLifecoursesResponse.ApiCall.Success)
                {
                    Log.Warn($"Error while updating {typeof(T)}. First update: {updates.First().Item1}. Debug information: {bulkUpdateLifecoursesResponse.DebugInformation}");
                }
            }
            catch(Exception e)
            {
                Log.Warn($"Could not update lifecourses for a batch {e.Message}");
            }
        }

        /// <summary>
        /// Creates a snapshot of the indices in Elasticsearch
        /// Throws an exception if the request could not be completed
        /// </summary>
        public void CreateSnapshot()
        {
            
            var date = DateTime.Now.ToString("dd-MM-yyyy_hh-mm-ss");
            var snapshot_name = $"snapshot_{date}";

            Log.Info($"Creating snapshot {snapshot_name}");

            var request = new SnapshotRequest("s3_repository", snapshot_name);

            var response = _esClient.Snapshot.Snapshot(request);

            if (!response.Accepted)
            {
                throw new Exception($"Could not create snapshot {snapshot_name}: " + response.ServerError);
            }
        }
        /// <summary>
        /// Creates or updates a S3 snapshot repository.
        /// </summary>
        public void CreateRepository()
        {
            var request = new CreateRepositoryRequest("s3_repository");
            request.Repository = new S3Repository(new S3RepositorySettings("link-lives-elasticsearch-snapshots"));

            Log.Info($"Creating repository s3_repository");

            var response = _esClient.Snapshot.CreateRepository(request);

            if (!response.Acknowledged)
            {
                throw new Exception($"Could not create repository s3_repository: " + response.ServerError);
            }
        }
    }
}
