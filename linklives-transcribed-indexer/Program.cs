using Linklives.Domain;
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
        private static ILog Log = LogManager.GetLogger(System.Reflection.MethodInfo.GetCurrentMethod().DeclaringType.Name);
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
            indexHelper.BulkIndexDocs<dynamic>(GetPas(path), indexName);
            indextimer.Stop();
            Log.Info($"Finished indexing all avilable files. Took: {indextimer.Elapsed}");
            Log.Info($"Activating new index: {indexName}");
            indexHelper.ActivateNewIndex(indexAlias, indexName);
        }

        private static IEnumerable<TranscribedPA> GetPas(string path)
        {
            var sources = GetSources();
            foreach (var source in sources)
            {
                var filepath = $"{path}\\{source.File_reference}";
                Log.Debug($"Reading PAs from file {filepath}");
                foreach (var item in new DataSet<dynamic>(filepath).Read())
                {
                    var pa = new TranscribedPA (item, source.Source_id);
                    pa.InitKey();
                    yield return pa;
                }
            }
        }
        private static IEnumerable<Source> GetSources()
        {
            //TODO: Get these from the new sources file when it becomes available, instead of hardcoding them.
            yield return new Source { Source_id = 0, File_reference = "transcribed_sources\\census\\1787_20190000.csv" };
            yield return new Source { Source_id = 1, File_reference = "transcribed_sources\\census\\1801_20190000.csv" };
            yield return new Source { Source_id = 2, File_reference = "transcribed_sources\\census\\1834_20190000.csv" };
            yield return new Source { Source_id = 3, File_reference = "transcribed_sources\\census\\1840_20190000.csv" };
            yield return new Source { Source_id = 4, File_reference = "transcribed_sources\\census\\1845_20190000.csv" };
            yield return new Source { Source_id = 5, File_reference = "transcribed_sources\\census\\1850_20190000.csv" };
            yield return new Source { Source_id = 6, File_reference = "transcribed_sources\\census\\1860_20190000.csv" };
            yield return new Source { Source_id = 7, File_reference = "transcribed_sources\\census\\1880_20190000.csv" };
            yield return new Source { Source_id = 8, File_reference = "transcribed_sources\\census\\1885_20190000.csv" };
            yield return new Source { Source_id = 9, File_reference = "transcribed_sources\\census\\1901_20190000.csv" };

            yield return new Source { Source_id = 10, File_reference = "transcribed_sources\\CBP\\CBP_20210309.csv" };

            yield return new Source { Source_id = 11, File_reference = "transcribed_sources\\PR\\by_PA\\burial.csv" };
            yield return new Source { Source_id = 12, File_reference = "transcribed_sources\\PR\\by_PA\\baptism.csv" };
            yield return new Source { Source_id = 13, File_reference = "transcribed_sources\\PR\\by_PA\\marriage.csv" };
            yield return new Source { Source_id = 14, File_reference = "transcribed_sources\\PR\\by_PA\\confirmation.csv" };
            yield return new Source { Source_id = 15, File_reference = "transcribed_sources\\PR\\by_PA\\departure.csv" };
            yield return new Source { Source_id = 16, File_reference = "transcribed_sources\\PR\\by_PA\\arrival.csv" };

        }
    }
}
