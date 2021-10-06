using Linklives.Indexer.Domain;
using Linklives.Indexer.Utils;
using log4net;
using log4net.Config;
using Nest;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Linklives.Indexer.Transcribed
{
    class Program
    {
        private static ILog Log;
        static int Main(string[] args)
        {
            Initconfig();
            var cmd = new RootCommand
            {
                new Option<string>("--path", "The path to the datasets top level folder"),
                new Option<string>("--es-host", "The url of the elastic search server to use for this indexation"),
                new Option<int>("--max-entries", getDefaultValue: ()=> 0, "the maximum ammount of entries to index, 0 indicates that all entries should be indexed."),
            };

            cmd.Handler = CommandHandler.Create<string, string, int>(Index);

            return cmd.Invoke(args);
        }
        private static void Initconfig()
        {
            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));
            Log = LogManager.GetLogger(System.Reflection.MethodInfo.GetCurrentMethod().DeclaringType.Name);
        }
        static void Index(string path, string esHost, int maxEntries)
        {
            var indexAlias = "transcribed";
            var esClient = new ElasticClient(new ConnectionSettings(new Uri(esHost))
               .RequestTimeout(TimeSpan.FromMinutes(2))
               .DisableDirectStreaming());
            var indexHelper = new ESHelper(esClient);
            Log.Info($"Creating new elasticsearch index for alias {indexAlias}");
            var indexName = indexHelper.CreateNewIndex<dynamic>(indexAlias, false);
            Log.Info($"Index {indexName} created");
            var indextimer = Stopwatch.StartNew();
            foreach (var dataset in GetDataSets(path))
            {
                var datasetTimer = Stopwatch.StartNew();
                Log.Info($"----------Beginning indexation of {dataset.FileName}----------");
                indexHelper.BulkIndexDocs<dynamic>(dataset.Read(), indexName);
                datasetTimer.Stop();
                Log.Info($"Finished indexing {dataset.FileName}. took: {datasetTimer.Elapsed}");
            }
            indextimer.Stop();
            Log.Info($"Finished indexing all avilable files. Took: {indextimer.Elapsed}");
            Log.Info($"Activating new index: {indexName}");
            indexHelper.ActivateNewIndex(indexAlias, indexName);
        }

        private static IEnumerable<DataSet<dynamic>> GetDataSets(string path)
        {
            var files = Directory.EnumerateFiles($"{path}\\transcribed_sources\\CBP", "*.csv", SearchOption.TopDirectoryOnly);
            files = files.Concat(Directory.EnumerateFiles($"{path}\\transcribed_sources\\census", "*.csv", SearchOption.TopDirectoryOnly));
            files = files.Concat(Directory.EnumerateFiles($"{path}\\transcribed_sources\\PR\\by_PA", "*.csv", SearchOption.TopDirectoryOnly));
            foreach (var file in files)
            {
                yield return new DataSet<dynamic>(file);
            }
        }
    }
}
