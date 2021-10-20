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
                new Option<string>("--path", "The path to the datasets top level folder"),
                new Option<string>("--es-host", "The url of the elastic search server to use for this indexation"),
                new Option<string>("--db-conn", "The url of the linklives api server to use for this indexation"),
                new Option<int>("--max-entries", getDefaultValue: ()=> 0, "the maximum ammount of entries to index, 0 indicates that all entries should be indexed."),
            };

            cmd.Handler = CommandHandler.Create<string, string, string, int>(Index);

            return cmd.Invoke(args);
        }
        private static void Initconfig()
        {
            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));
        }
        static void Index(string path, string esHost, string dbConn, int maxEntries)
        {
            #region ES Setup
            var esClient = new ElasticClient(new ConnectionSettings(new Uri(esHost))
               .RequestTimeout(TimeSpan.FromMinutes(2))
               .DisableDirectStreaming());
            var indexHelper = new ESHelper(esClient);
            #endregion
            #region EF Setup
            var transcribedPARepository = new ESTranscribedPaRepository(esClient);
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
            Log.Info("Indexing lifecourses");
            var lifecourses = maxEntries == 0 ? ReadLifeCourses(path) : ReadLifeCourses(path).Take(maxEntries).ToList();
            indexHelper.BulkIndexDocs(lifecourses, AliasIndexMapping["lifecourses"]);
            Log.Info($"Finished indexing lifecourses. took {datasetTimer.Elapsed}");
            datasetTimer.Restart();
            Log.Info("Inserting lifecourses to DB");
            var lifecourseRepo = new EFLifeCourseRepository(dbContext);
            int count = 0;
            foreach (var batch in lifecourses.Batch(5000))
            {
                var uniqueEntitites = batch.GroupBy(x => x.Key).Select(x => x.First()); //Guard against duplicate lifecourses. 
                Log.Debug($"Upserting batch #{count} containing {uniqueEntitites.Count()} lifecourses to db");
                lifecourseRepo.Upsert(uniqueEntitites);
                lifecourseRepo.Save();
                count++;
            }
            Log.Info($"Finished inserting lifecourses to db. took {datasetTimer.Elapsed}");
            datasetTimer.Restart();

            Log.Info("Indexing person appearances");
            var pas = maxEntries == 0 ? ReadPAs(path, transcribedPARepository) : ReadPAs(path, transcribedPARepository).Where(p => lifecourses.SelectMany(lc => lc.Links.SelectMany(l => l.PaKeys)).ToList().Contains(p.Key));
            indexHelper.BulkIndexDocs(pas, AliasIndexMapping["pas"]);
            Log.Info($"Finished indexing person appearances. took {datasetTimer.Elapsed}");
            datasetTimer.Restart();

            Log.Info("Indexing sources");
            indexHelper.BulkIndexDocs(new DataSet<Source>($"{path}\\auxilary_data\\sources\\sources.csv").Read(), AliasIndexMapping["sources"]);
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
        private static IEnumerable<BasePA> ReadPAs(string basePath, ITranscribedPARepository transcribedPARepository)
        {
            var sources = new DataSet<Source>($"{basePath}\\auxilary_data\\sources\\sources.csv");
            foreach (var source in sources.Read())
            {
                Log.Debug($"Reading PAs from source {source.Source_name}");
                var paSet = new DataSet<StandardPA>($"{basePath}\\{source.File_reference}");
                var transcribedSet = transcribedPARepository.GetBySource(source.Source_id);
                foreach (var stdPa in paSet.Read())
                {
                    BasePA pa = null;
                    try
                    {
                        pa = BasePA.Create(source.Source_id, stdPa, transcribedSet.First(t => t.Pa_id == stdPa.Pa_id));
                        pa.InitKey();                        
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                    yield return pa;
                }
            }
        }
        private static IEnumerable<LifeCourse> ReadLifeCourses(string basepath)
        {
            var lifecoursesDataset = new DataSet<LifeCourse>($"{basepath}\\life-courses\\life_courses.csv");
            var links = MakeLinksUnique(new DataSet<Link>($"{basepath}\\links\\links.csv").Read(true).ToList());
            foreach (var lifecourse in lifecoursesDataset.Read())
            {
                var linkIds = lifecourse.Link_ids.Split(',').Select(i => i);
                lifecourse.Links = links.Where(l => linkIds.Intersect(l.Link_id.Split(",")).Any()).ToList();
                lifecourse.InitKey();
                yield return lifecourse;
            }
        }
        private static IEnumerable<Link> MakeLinksUnique(IEnumerable<Link> links)
        {
            var groups = links.GroupBy(l => l.Key);
            foreach (var group in groups)
            {
                var link = group.First();
                if (group.Count() > 1)
                {
                    link.Link_id = string.Join(',', group.Select(l => l.Link_id));
                }
                yield return link;
            }
        }
    }
}
