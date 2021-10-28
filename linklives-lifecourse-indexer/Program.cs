using Linklives.DAL;
using Linklives.Domain;
using Linklives.Indexer.Domain;
using Linklives.Indexer.Utils;
using log4net;
using log4net.Config;
using Microsoft.EntityFrameworkCore;
using MoreLinq;
using Nest;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Z.EntityFramework.Extensions;

namespace Linklives.Indexer.Lifecourses
{
    class Program
    {
        private static ILog Log = LogManager.GetLogger(System.Reflection.MethodInfo.GetCurrentMethod().DeclaringType.Name);
        static int Main(string[] args)
        {
            Initconfig();
            var cmd = new RootCommand
            {
                new Option<string>("--ll-path", "The path to the datasets top level folder"),
                new Option<string>("--trs-path", "The path to the datasets top level folder"),
                new Option<string>("--es-host", "The url of the elastic search server to use for this indexation"),
                new Option<string>("--db-conn", "The url of the linklives api server to use for this indexation"),
                new Option<int>("--max-entries", getDefaultValue: ()=> 0, "the maximum ammount of entries to index, 0 indicates that all entries should be indexed."),
            };

            cmd.Handler = CommandHandler.Create<string, string, string, string, int>(Index);

            return cmd.Invoke(args);
        }
        private static void Initconfig()
        {
            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));
        }
        static void Index(string llPath, string trsPath, string esHost, string dbConn, int maxEntries)
        {
            #region ES Setup
            var esClient = new ElasticClient(new ConnectionSettings(new Uri(esHost))
               .RequestTimeout(TimeSpan.FromMinutes(4))
               .DisableDirectStreaming());
            var indexHelper = new ESHelper(esClient);
            var transcribedPARepository = new ESTranscribedPaRepository(esClient);
            #endregion
            #region EF Setup
            //This context factory is required by the EF extensions used in linklives.lib for bulk upserts
            EntityFrameworkManager.ContextFactory = context =>
            {
                var optionsBuilder = new DbContextOptionsBuilder<LinklivesContext>();
                optionsBuilder.UseMySQL(dbConn);
                optionsBuilder.EnableSensitiveDataLogging();
                return new LinklivesContext(optionsBuilder.Options);
            };
            var dbContext = (LinklivesContext)EntityFrameworkManager.ContextFactory.Invoke(null);
            #endregion

            var AliasIndexMapping = SetUpNewIndexes(indexHelper);
            var indextimer = Stopwatch.StartNew();
            var datasetTimer = Stopwatch.StartNew();
            Log.Info("Reading lifecourses from file, this may take some time...");
            var lifecourses = maxEntries == 0 ? ReadLifeCourses(llPath).ToList() : ReadLifeCourses(llPath).Take(maxEntries).ToList();
            Log.Info("Indexing lifecourses");
            indexHelper.BulkIndexDocs(lifecourses, AliasIndexMapping["lifecourses"]);
            Log.Info($"Finished indexing lifecourses. took {datasetTimer.Elapsed}");
            datasetTimer.Restart();
            Log.Info("Inserting lifecourses to DB");
            var beforecount = lifecourses.Count();
            lifecourses = lifecourses.GroupBy(x => x.Key).Select(x => x.First()).ToList();
            Log.Info($"Discarded {beforecount - lifecourses.Count()} lifecourses while checking for duplicate keys. Took {datasetTimer.Elapsed}");
            var lifecourseRepo = new EFLifeCourseRepository(dbContext);
            int count = 1;
            foreach (var batch in lifecourses.Batch(10000))
            {
                var timer = Stopwatch.StartNew();
                lifecourseRepo.Upsert(batch);
                lifecourseRepo.Save();
                Log.Debug($"Upserted batch #{count} containing {batch.Count()} lifecourses to db. Took {timer.Elapsed}");
                count++;
            }
            lifecourses.Clear(); //free up some memory space
            Log.Info($"Finished inserting lifecourses to db. took {datasetTimer.Elapsed}");
            datasetTimer.Restart();
            Log.Info("Indexing person appearances");
            var sources = new DataSet<Source>($"{llPath}\\auxilary_data\\sources\\sources.csv").Read().ToList();
            //Parallel.ForEach(sources, new ParallelOptions { MaxDegreeOfParallelism = 2 }, source =>
            foreach (var source in sources)
            {
                Log.Debug($"Reading PAs from source {source.Source_name}");
                var timer = Stopwatch.StartNew();
                var sourcePAs = ReadSourcePAs(llPath, source, trsPath);
                if (maxEntries != 0)
                {
                    //TODO: could we improve performance by using joins instead?
                    sourcePAs = sourcePAs.Where(p => lifecourses.SelectMany(lc => lc.Links.SelectMany(l => l.PaKeys)).ToList().Contains(p.Key));
                }
                indexHelper.BulkIndexDocs(sourcePAs, AliasIndexMapping["pas"]);
                Log.Debug($"finished fetching PAs from source {source.Source_name}. Took: {timer.Elapsed}");
            }//);
            Log.Info($"Finished indexing person appearances. took {datasetTimer.Elapsed}");
            datasetTimer.Restart();

            Log.Info("Indexing sources");
            indexHelper.BulkIndexDocs(sources, AliasIndexMapping["sources"]);
            Log.Info($"Finished indexing sources. took {datasetTimer.Elapsed}");
            datasetTimer.Stop();

            indextimer.Stop();
            Log.Info($"Finished indexing all avilable files. Took: {indextimer.Elapsed}");
            Log.Info($"Activating new indices");
            indexHelper.ActivateNewIndices(AliasIndexMapping);
        }

        private static IDictionary<string, string> SetUpNewIndexes(ESHelper indexHelper)
        {
            var result = new Dictionary<string, string>();
            result["pas"] = indexHelper.CreateNewIndex<BasePA>("pas");
            //result["links"] = indexHelper.CreateNewIndex<Link>("links");
            result["lifecourses"] = indexHelper.CreateNewIndex<LifeCourse>("lifecourses");
            result["sources"] = indexHelper.CreateNewIndex<Source>("sources");
            return result;
        }
        private static IEnumerable<BasePA> ReadSourcePAs(string basePath, Source source, string trsPath)
        {            
            var paDict = new DataSet<StandardPA>($"{basePath}\\{source.File_reference}").Read().ToDictionary(x => x.Pa_id);
            var trsSet = new DataSet<dynamic>($"{trsPath}\\{source.Original_data_reference}");
            //Transcribed files can be pretty big so going over them row by row when matching to our standardised pa saves on memory.
            foreach (var transcribtion in trsSet.Read())
            {
                BasePA pa = null;
                try
                {
                    var trsPa = new TranscribedPA(transcribtion, source.Source_id);
                    pa = BasePA.Create(source, paDict[trsPa.Pa_id], trsPa);
                    pa.InitKey();                    
                }
                catch (Exception e)
                {
                    //If for some reason we cant parse a PA we skip it and log the error.
                    Log.Error($"Failed to read pa with id {transcribtion.pa_id} reason: {e.Message}", e);
                    continue;
                }
                yield return pa;
            }
        }
        private static IEnumerable<LifeCourse> ReadLifeCourses(string basepath)
        {
            var lifecoursesDataset = new DataSet<LifeCourse>($"{basepath}\\life-courses\\life_courses.csv");
            Log.Debug("Uniquefying links");
            var timer = Stopwatch.StartNew();
            var links = MakeLinksUnique(new DataSet<Link>($"{basepath}\\links\\links.csv").Read(true));
            Log.Debug($"Finished uniquefying links. Took: {timer.Elapsed}");
            timer.Stop();
            foreach (var lifecourse in lifecoursesDataset.Read())
            {
                //TODO: Can we find the links in one go instead of doing 2 queries? It might perform better.
                var linkIds = lifecourse.Link_ids.Split(',');
                lifecourse.Links = new List<Link>();
                foreach (var id in linkIds)
                {
                    lifecourse.Links.Add(links[id]);
                }
                lifecourse.InitKey();
                yield return lifecourse;
            }
        }
        private static Dictionary<string, Link> MakeLinksUnique(IEnumerable<Link> links)
        {
            var groups = links.GroupBy(l => l.Key);
            var result = new Dictionary<string, Link>();
            foreach (var group in groups)
            {
                var mainLink = group.First();
                mainLink.Link_id = string.Join(',', group.Select(l => l.Link_id));
                foreach (var link in group)
                {
                    result.Add(link.Link_id, mainLink);
                }
            }
            return result;
        }
    }
}
