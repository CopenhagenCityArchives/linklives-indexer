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
               .RequestTimeout(TimeSpan.FromMinutes(1))
               .DisableDirectStreaming());
            var indexHelper = new ESHelper(esClient);
            var transcribedPARepository = new ESTranscribedPaRepository(esClient);
            #endregion
            #region EF Setup
            var optionsBuilder = new DbContextOptionsBuilder<LinklivesContext>();
            optionsBuilder.UseMySQL(dbConn);
            optionsBuilder.EnableSensitiveDataLogging();
            //This context factory is required by the EF extensions used in linklives.lib for bulk upserts
            EntityFrameworkManager.ContextFactory = context =>
            {

                return new LinklivesContext(optionsBuilder.Options);
            };
            var dbContext = (LinklivesContext)EntityFrameworkManager.ContextFactory.Invoke(null);
            #endregion
            
                var AliasIndexMapping = SetUpNewIndexes(indexHelper);
                var indextimer = Stopwatch.StartNew();
                var datasetTimer = Stopwatch.StartNew();
                Log.Info("Reading lifecourses from file, this may take some time...");
                var lifecourses = maxEntries == 0 ? ReadLifecoursesAndLinks(llPath).ToList() : ReadLifecoursesAndLinks(llPath, maxEntries).ToList();
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
                foreach (var batch in lifecourses.Batch(200))
                {
                    var timer = Stopwatch.StartNew();
                      
                    BatchInsertDBRows<LifeCourse>(optionsBuilder.Options, batch);
                      
                    Log.Debug($"Upserted batch #{count} containing {batch.Count()} lifecourses to db. Took {timer.Elapsed}");
                    count++;
                }
            
                var pasInLifeCourses = new Dictionary<string, int>();

                // Build a list of pa-ids in lifecourses if max-entries is set
                if(maxEntries > 0)
                {
                    foreach (LifeCourse lc in lifecourses)
                    {
                        foreach(string paKey in lc.Source_ids.Split(",").Zip(lc.Pa_ids.Split(","), (first, second) => first + "-" + second))
                        {
                            pasInLifeCourses.TryAdd(paKey, lc.Life_course_id);
                        }
                    }
                }

                lifecourses.Clear(); //free up some memory space
                Log.Info($"Finished inserting lifecourses to db. took {datasetTimer.Elapsed}");
                datasetTimer.Restart();
      
            try
            { 
                Log.Info("Indexing person appearances");
                var sources = new DataSet<Source>(Path.Combine(llPath, "auxilary_data", "sources", "sources.csv")).Read().ToList();
                Parallel.ForEach(sources, new ParallelOptions { MaxDegreeOfParallelism = 6 }, source =>
                //foreach (var source in sources)
                //var source = sources[8];
                {
                    Log.Debug($"Reading PAs from source {source.Source_name}");
                    var timer = Stopwatch.StartNew();
                    var sourcePAs = ReadSourcePAs(llPath, source, trsPath, pasInLifeCourses);
                    Log.Debug($"Indexing PAs from source {source.Source_name}");
                    //indexHelper.BulkIndexDocs(sourcePAs, AliasIndexMapping["pas"]);
                    var paBatch = new List<BasePA>();
                    foreach(var curPa in sourcePAs)
                    {
                        paBatch.Add(curPa);
                        if(paBatch.Count == 3000)
                        {
                            indexHelper.IndexManyDocs(paBatch, AliasIndexMapping["pas"]);
                            UpdateLifecourses(esClient, paBatch, pasInLifeCourses, AliasIndexMapping["lifecourses"]);

                            foreach (var pa in paBatch)
                            {
                                pasInLifeCourses.Remove(pa.Key);
                            }
                            paBatch.Clear();
                        }
                    }

                    // End of iteration, flush
                    if(paBatch.Count > 0)
                    {
                        indexHelper.IndexManyDocs(paBatch, AliasIndexMapping["pas"]);
                        UpdateLifecourses(esClient, paBatch, pasInLifeCourses, AliasIndexMapping["lifecourses"]);

                        foreach (var pa in paBatch) {
                            pasInLifeCourses.Remove(pa.Key);
                        }
                        paBatch.Clear();
                    }

                    Log.Debug($"Finished indexing PAs from source {source.Source_name}. Took: {timer.Elapsed}");
                    Log.Debug($"PAs in lifecourses remaining: {pasInLifeCourses.Count}");
                }
                );
                Log.Info($"Finished indexing person appearances. Took {datasetTimer.Elapsed}");
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
            catch(Exception e)
            {
                Log.Warn("Could not complete indexation: " + e.Message);
                Log.Info("Removing new indexes");
                foreach(var mapping in AliasIndexMapping){
                    Log.Info($"Removing index {mapping.Value}");
                    indexHelper.RemoveIndex(mapping.Value);
                }
            }
        }

        //TODO Document
        private static void BatchInsertDBRows<T>(DbContextOptions<LinklivesContext> contextOptions, IEnumerable<LifeCourse> entities) where T: class
        {
            LinklivesContext localContext = new LinklivesContext(contextOptions);
            localContext.ChangeTracker.AutoDetectChangesEnabled = false;

            var newEntitiesIDs = entities.Select(u => u.Key).Distinct().ToArray();
            var entitiesInDb = localContext.LifeCourses.Load();
                Where(u => newEntitiesIDs.Contains(u.Key))
                                           .Select(u => u.Key).ToArray();
            var usersNotInDb = entities.Where(u => !entitiesInDb.Contains(u.Key));
            foreach (LifeCourse lc in usersNotInDb)
            {
                localContext.Add(lc);
            }

            localContext.SaveChanges();

            localContext.Dispose();
        }

        private static void UpdateLifecourses(ElasticClient esClient, IEnumerable<BasePA> paBatch, IDictionary<string, int> pasInLifeCourses, string index)
        {
            var updates = new List<Tuple<int, BasePA>>();

            foreach (BasePA pa in paBatch)
            {
                if (pasInLifeCourses.ContainsKey(pa.Key))
                {
                    updates.Add(new Tuple<int, BasePA>(pasInLifeCourses[pa.Key], pa));
                }
            }
            Log.Debug($"Updating {updates.Count} lifecourses with pas");
            var bulkUpdateLifecoursesResponse = esClient.Bulk(b => b
                                .Index(index)
                                .UpdateMany(updates, (descriptor, update) => descriptor
                                    .Id(update.Item1)
                                    .Script(s => s
                                        .Source("ctx._source.person_appearance.add(params.pa)")
                                        .Params(p => p
                                            .Add("pa", update.Item2)
                                        )
                                    )
                                )
                            );

            if (bulkUpdateLifecoursesResponse.Errors)
            {
                Log.Warn($"Could not index lifecourses for a batch {bulkUpdateLifecoursesResponse.DebugInformation}");
            }
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
        private static IEnumerable<BasePA> ReadSourcePAs(string basePath, Source source, string trsPath, Dictionary<string,int> paFilter)
        {
            Log.Debug($"Loading standardized PAs into memory from {Path.Combine(basePath, source.File_reference)}");
            var paDict = new DataSet<StandardPA>(Path.Combine(basePath, source.File_reference)).Read().Where(x => paFilter.Count == 0 || paFilter.ContainsKey($"{source.Source_id}-{x.Pa_id}")).ToDictionary(x => x.Pa_id);
            if (paDict.Count == 0)
            {
                Log.Debug($"No standardized PAs matched the paFilter, skipping this source");
                yield break;
            }
            #region skip transcribed
            foreach (KeyValuePair<int, StandardPA> spa in paDict)
            {
                var pa = BasePA.Create(source, spa.Value, null);
                pa.InitKey();
                if (paFilter.Count > 0 && !paFilter.ContainsKey(pa.Key)) { continue; }
                yield return pa;
            }
            #endregion
            

            #region use transcribed
            /*
            Log.Debug($"Reading transcribed PAs from {Path.Combine(trsPath, source.Original_data_reference)}");
            var trsSet = new DataSet<dynamic>(Path.Combine(trsPath, source.Original_data_reference));
            //Transcribed files can be pretty big so going over them row by row when matching to our standardised pa saves on memory.
            foreach (var transcribtion in trsSet.Read())
            {
                BasePA pa = null;
                try
                {
                    var trsPa = new TranscribedPA(transcribtion, source.Source_id);
                    if(!paDict.ContainsKey(trsPa.Pa_id)) { continue; }
                    pa = BasePA.Create(source, paDict[trsPa.Pa_id], trsPa);
                    pa.InitKey();          
                }
                catch (Exception e)
                {
                    //If for some reason we cant parse a PA we skip it and log the error.
                    Log.Error($"Failed to read pa with id {transcribtion.Pa_id} reason: {e.Message}", e);
                    continue;
                }

                //If paFilter has entries and the pa is not in it, dont return it
                if (paFilter.Count > 0 && !paFilter.ContainsKey(pa.Key)) { continue; }

                yield return pa;
            }
            */
            #endregion
        }
        private static IEnumerable<LifeCourse> ReadLifecoursesAndLinks(string basepath, int lifecourseCount = 0)
        { 
            Log.Debug($"Reading lifecourses into memory from {Path.Combine(basepath, "life - courses", "life_courses.csv")}");
            var lifecoursesDataset = new DataSet<LifeCourse>(Path.Combine(basepath, "life-courses", "life_courses.csv"));
            int rowsRead = 0;
            var linkIdsInLifecourses = new Dictionary<string,bool>();
            var lifecourses = new List<LifeCourse>();
            foreach (var lifecourse in lifecoursesDataset.Read())
            {
                rowsRead++;
                if(lifecourseCount != 0 && rowsRead > lifecourseCount) { break; }
                foreach(string linkId in lifecourse.Link_ids.Split(','))
                {
                    linkIdsInLifecourses.TryAdd(linkId, true);
                }
                lifecourses.Add(lifecourse);
            }

            Log.Debug("Reading links");
            var linksDataset = new DataSet<Link>(Path.Combine(basepath, "links", "links.csv"));
            var links = new List<Link>();
            foreach (var link in linksDataset.Read())
            {
                if (linkIdsInLifecourses.ContainsKey(link.Link_id))
                {
                    link.InitKey();
                    links.Add(link);
                }
            }
            
            Log.Debug($"Uniquefying links");
            var timer = Stopwatch.StartNew();
            var uniqueLinks = MakeLinksUnique(links);
            links.Clear();
            linkIdsInLifecourses.Clear();
            Log.Debug($"Finished uniquefying links. Took: {timer.Elapsed}");
            timer.Stop();

            Log.Debug($"Combining lifecourses and links");
            foreach (var lifecourse in lifecourses)
            {
                lifecourse.Links = new List<Link>();
                var linkIds = lifecourse.Link_ids.Split(',');
                foreach (var id in linkIds)
                {
                    lifecourse.Links.Add(uniqueLinks[id]);
                }
                lifecourse.InitKey();
                yield return lifecourse;
            }
        }
        private static Dictionary<string, Link> MakeLinksUnique(List<Link> links)
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