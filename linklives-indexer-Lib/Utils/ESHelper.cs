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
        public string CreateNewIndex<T>(string index) where T : class
        {
            var indexname = $"{index}_{DateTime.Now.ToString("dd-MM-yyyy_hh-mm-ss")}";
            _esClient.Indices.Create(indexname, c => c.
                Map<T>(m => m.AutoMap()));
            return indexname;
        }
        public void ActivateNewIndices(IDictionary<string, string> indexMappings)
        {
            foreach (var indexMap in indexMappings)
            {
                ActivateNewIndex(indexMap.Key, indexMap.Value);
            }
        }
        public void ActivateNewIndex(string index, string indexName)
        {
            //Cleanup existing aliases
            _esClient.Indices.DeleteAlias($"{index}*", "_all");
            //Add our new alias
            _esClient.Indices.PutAlias(index, indexName);
            //Find and delete all the old versions of this index
            var oldIndices = _esClient.Indices.Get($"{index}*").Indices.Where(i => i.Key.Name != indexName);
            foreach (var oldIndex in oldIndices)
            {
                _esClient.Indices.Delete(oldIndex.Key.Name);
            }
        }
        public void BulkIndexDocs<T>(IEnumerable<T> docs, string index) where T : class
        {
            //TODO: Add fetch some of these config values from enviroment config instead of hardcording.
            var bulkAllObservable = _esClient.BulkAll(docs, b => b
               .Index(index)
               .BackOffTime("30s")
               .BackOffRetries(3)
               .RefreshOnCompleted()
               .MaxDegreeOfParallelism(Environment.ProcessorCount)
               .Size(5000))
               .Wait(TimeSpan.FromHours(3), onNext: response => { Log.Info($"Page: {response.Page} containing: {response.Items.Count} items sucessfully indexed to {index}"); });
        }
    }
}
