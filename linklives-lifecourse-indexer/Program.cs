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
using System.IO;
using System.Reflection;

namespace Linklives.Indexer.Lifecourses
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
                new Option<string>("--api-host", "The url of the linklives api server to use for this indexation"),
                new Option<int>("--max-entries", getDefaultValue: ()=> 0, "the maximum ammount of entries to index, 0 indicates that all entries should be indexed."),
            };

            cmd.Handler = CommandHandler.Create<string, string, string, int>(Index);

            return cmd.Invoke(args);
        }
        private static void Initconfig()
        {
            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));
            Log = LogManager.GetLogger(System.Reflection.MethodInfo.GetCurrentMethod().DeclaringType.Name);
        }
        static void Index(string path, string esHost, string apiHost, int maxEntries)
        {
            var esClient = new ElasticClient(new ConnectionSettings(new Uri(esHost))
               .RequestTimeout(TimeSpan.FromMinutes(2))
               .DisableDirectStreaming());
            var indexHelper = new ESHelper(esClient);
            var AliasIndexMapping = SetUpNewIndexes(indexHelper);

            indexHelper.BulkIndexDocs(ReadPAs(path), AliasIndexMapping["pas"]);
            //Do all the indexing stuff
            indexHelper.ActivateNewIndices(AliasIndexMapping);
        }
        private static IDictionary<string,string> SetUpNewIndexes(ESHelper indexHelper)
        {
            var result = new Dictionary<string, string>();
            result["pas"] = indexHelper.CreateNewIndex<BasePA>("pas");
            result["links"] = indexHelper.CreateNewIndex<Link>("links");
            result["lifecourses"] = indexHelper.CreateNewIndex<LifeCourse>("lifecourses");
            result["sources"] = indexHelper.CreateNewIndex<Source>("sources");
            return result;
        }
        private static IEnumerable<BasePA> ReadPAs(string basePath)
        {
            var sources = new DataSet<Source>($"{basePath}\\auxilary_data\\sources\\sources.csv");
            foreach (var source in sources.Read())
            {
                var paSet = new DataSet<StandardPA>($"{basePath}\\{source.File_reference}");
                foreach (var stdPa in paSet.Read())
                {
                    BasePA pa = null;
                    try
                    {
                        pa = BasePA.Create(source.Source_id, stdPa);
                    }
                    catch (Exception)
                    {

                        throw;
                    }
                    yield return pa;
                }
            }
        }
    }
}
