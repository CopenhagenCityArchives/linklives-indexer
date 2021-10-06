using log4net;
using Nest;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
        public string CreateNewIndex<T>(string index, bool dateDetection = true) where T : class
        {
            var indexname = $"{index}_{DateTime.Now.ToString("dd-MM-yyyy_hh-mm-ss")}";
            _esClient.Indices.Create(indexname, c => c.
            Map<T>(m => m
                .DateDetection(dateDetection)
                .AutoMap()));

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
                Log.Debug($"Deleting {oldIndex}");
                _esClient.Indices.Delete(oldIndex.Key.Name);
            }
        }
        public void BulkIndexDocs<T>(IEnumerable<T> docs, string index) where T : class
        {
            //TODO: Add fetch some of these config values from enviroment config instead of hardcording.
            //TODO: Maybe pass in a proper oberserver so we can handle events other than onNext and leave handling of events to the calling code instead of doing it here. see: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/indexing-documents.html#_advanced_bulk_indexing
            var bulkAllObservable = _esClient.BulkAll(docs, b => b
               .Index(index)
               .BackOffTime("30s")
               .BackOffRetries(3)
               .RefreshOnCompleted()
               .MaxDegreeOfParallelism(Environment.ProcessorCount)
               .Size(5000))
               .Wait(TimeSpan.FromHours(3), onNext: response => { Log.Debug($"Page: {response.Page} containing: {response.Items.Count} items sucessfully indexed to {index}"); });

        }
    }
}
