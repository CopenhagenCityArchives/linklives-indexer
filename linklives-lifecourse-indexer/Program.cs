﻿using Linklives.DAL;
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
        private static string DataVersion;
        static int Main(string[] args)
        {
            Initconfig();
            var cmd = new RootCommand
            {
                new Option<string>("--ll-path", "The path to the datasets top level folder"),
                new Option<string>("--trs-path", "The path to the datasets top level folder"),
                new Option<string>("--es-host", "The url of the elastic search server to use for this indexation"),
                new Option<string>("--db-conn", "The url of the linklives api server to use for this indexation"),
                new Option<string>("--data-version", "the version of data from WP3"),
                new Option<bool>("--skip-db", getDefaultValue: ()=> false,"Skip database upserts of lifecourses and links"),
                new Option<bool>("--skip-es", getDefaultValue: ()=> false,"Skip Elasticsearch indexation of person appearances and sources"),
                new Option<int>("--max-entries", getDefaultValue: ()=> 0, "the maximum ammount of entries to index, 0 indicates that all entries should be indexed."),
            };

            cmd.Handler = CommandHandler.Create<string, string, string, string, string, bool, bool, int>(Index);

            return cmd.Invoke(args);
        }
        private static void Initconfig()
        {
            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));
        }
        static void Index(string llPath, string trsPath, string esHost, string dbConn, string dataVersion, bool skipDb, bool skipEs, int maxEntries)
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

            // Data version as given in WP3 data
            //TODO Set somewhere else
            DataVersion = dataVersion;
            if (string.IsNullOrEmpty(DataVersion)) { throw new Exception("DataVersion is not set. Cannot continue"); }

            var indextimer = Stopwatch.StartNew();
            var datasetTimer = Stopwatch.StartNew();
            Log.Info("Reading lifecourses");
            var lifecourses = maxEntries == 0 ? ReadLifecoursesAndLinks(llPath).ToList() : ReadLifecoursesAndLinks(llPath, maxEntries).ToList();
            if (!skipEs)
            {
                Log.Info("Indexing lifecourses");
                indexHelper.BulkIndexDocs(lifecourses, AliasIndexMapping["lifecourses"]);
                Log.Info($"Finished indexing lifecourses. took {datasetTimer.Elapsed}");
            }
            
            datasetTimer.Restart();
            Log.Info("Discarding duplicate lifecourses");
            var beforecount = lifecourses.Count();
            lifecourses = lifecourses.GroupBy(x => x.Key).Select(x => x.First()).ToList();
            Log.Info($"Discarded {beforecount - lifecourses.Count()} lifecourses while checking for duplicate keys. Took {datasetTimer.Elapsed}");

            datasetTimer.Restart();

            if (skipDb)
            {
                Log.Info("Skipping database upserts");
            }
            else
            {
                var lifecourseRepo = new EFLifeCourseRepository(dbContext, optionsBuilder.Options);
                Log.Info($"Upserting {lifecourses.Count()} lifecourses");
                lifecourseRepo.Upsert(lifecourses, DataVersion);
                Log.Info($"Marking old lifecourses");
                lifecourseRepo.MarkOldItems(lifecourses);
                Log.Info($"Done upserting. Took {datasetTimer.Elapsed}");
            }
            
            try
            { 
                var sources = new DataSet<Source>(Path.Combine(llPath, "auxilary_data", "sources", "sources.csv")).Read().ToList();
                var missingSourceFiles = new List<string>();
                foreach(var source in sources)
                {
                    var path = Path.Combine(llPath, source.File_reference);
                    if (!File.Exists(path))
                    {
                        missingSourceFiles.Add(path);
                    }
                }

                if(missingSourceFiles.Count > 0)
                {
                    throw new Exception("Error checking sources files. The following files does not exist:" + string.Join(",", missingSourceFiles));
                }

                if (skipEs)
                {
                    Log.Info("Skipping indexation of person apperances");
                }
                else
                {
                    var pasInLifeCourses = new Dictionary<string, List<string>>();

                    // Build a list of pa-ids in lifecourses
                    Log.Info($"Building pasInLifecourses dictionary");
                    datasetTimer.Restart();
                    foreach (LifeCourse lc in lifecourses)
                    {
                        foreach (string paKey in lc.GetPAKeys())
                        {
                            if (!pasInLifeCourses.ContainsKey(paKey))
                            {
                                pasInLifeCourses.Add(paKey, new List<string>() { lc.Key });
                            }
                            else
                            {
                                pasInLifeCourses[paKey].Add(lc.Key);
                            }
                        }
                    }

                    Log.Info($"Finished building pasInLifecourses dictionary. Took {datasetTimer.Elapsed}");
                    datasetTimer.Restart();

                    string lifecourseUpdateScript = 
                            "if(ctx._source.event_year_sortable < params.pa.event_year_sortable)" +
                                "{ " +
                                    "ctx._source.sourceyear_sortable = params.pa.sourceyear_sortable;" +
                                    "ctx._source.first_names_sortable = params.pa.first_names_sortable;" +
                                    "ctx._source.family_names_sortable = params.pa.family_names_sortable;" +
                                    "ctx._source.birthyear_sortable = params.pa.birthyear_sortable;" +
                                    "ctx._source.event_year_sortable = params.pa.event_year_sortable;" +
                                    "ctx._source.deathyear_sortable = params.pa.deathyear_sortable;" +
                                "}" +
                                "ctx._source.person_appearance.add(params.pa);";

                    Log.Info("Indexing person appearances");

                    Parallel.ForEach(sources, new ParallelOptions { MaxDegreeOfParallelism = 2 }, source =>
                    {
                        Log.Info($"Reading PAs from source {source.Source_name}");
                        var timer = Stopwatch.StartNew();
                        var sourcePAs = ReadSourcePAs(llPath, source, trsPath, pasInLifeCourses, maxEntries != 0);
                        Log.Info($"Indexing PAs from source {source.Source_name}");
                        //indexHelper.BulkIndexDocs(sourcePAs, AliasIndexMapping["pas"]);
                        var paBatch = new List<BasePA>();
                        var lifecourseUpdates = new List<Tuple<string, BasePA>>();
                        int pasIndexed = 0;
                        int lifecoursesUpdated = 0;

                        foreach (var curPa in sourcePAs)
                        {
                            pasIndexed++;

                            paBatch.Add(curPa);
                            if (paBatch.Count == 3000)
                            {
                                indexHelper.IndexManyDocs(paBatch, AliasIndexMapping["pas"]);
                                paBatch.Clear();
                            }

                            AddLifeCourseUpdates(curPa, lifecourseUpdates, pasInLifeCourses);
                            if (lifecourseUpdates.Count > 1000)
                            {
                                indexHelper.UpdateMany(lifecourseUpdateScript, lifecourseUpdates, AliasIndexMapping["lifecourses"]);
                                lifecoursesUpdated += lifecourseUpdates.Count;
                                lifecourseUpdates.Clear();
                            }

                            if (pasIndexed % 100000 == 0)
                            {
                                Log.Info($"Indexed {pasIndexed} from {source.Source_name}");
                            }
                        }

                        // End of iteration, flush
                        if (paBatch.Count > 0)
                        {
                            indexHelper.IndexManyDocs(paBatch, AliasIndexMapping["pas"]);
                            paBatch.Clear();
                        }

                        if(lifecourseUpdates.Count > 0)
                        {
                            indexHelper.UpdateMany(lifecourseUpdateScript, lifecourseUpdates, AliasIndexMapping["lifecourses"]);
                            lifecoursesUpdated += lifecourseUpdates.Count;
                            lifecourseUpdates.Clear();
                        }

                        Log.Info($"Finished indexing {pasIndexed} PAs and updating {lifecoursesUpdated} lifecourses from source {source.Source_name}. Took: {timer.Elapsed}");
                    });

                    Log.Info($"Finished indexing PAs. Took {datasetTimer.Elapsed}");
                    datasetTimer.Restart();

                    Log.Info("Indexing sources");
                    indexHelper.BulkIndexDocs(sources, AliasIndexMapping["sources"]);
                    Log.Info($"Finished indexing sources. took {datasetTimer.Elapsed}");
                    datasetTimer.Stop();

                    indextimer.Stop();
                    Log.Info($"Finished indexing all avilable files. Took: {indextimer.Elapsed}");
                    Log.Info($"Activating new indices");
                    indexHelper.ActivateNewIndices(AliasIndexMapping);

                    try
                    {
                        Log.Info("Creating repository if is does not exist");
                        indexHelper.CreateRepository();

                        Log.Info("Creating snapshot of all indices");
                        indexHelper.CreateSnapshot("pas_lifecourses_sources", AliasIndexMapping.Select(am => am.Key).ToList());
                    }
                    catch(Exception e)
                    {
                        Log.Warn("Could not create snapshot from Elasticsearch: " + e.Message);
                    }

                    Log.Info($"All done");
                }
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

        private static void AddLifeCourseUpdates(BasePA pa, List<Tuple<string, BasePA>> updates, Dictionary<string,List<string>> pasInLifeCourses)
        {
            // If the PA is not in pasInLifecourses, the given PA should not trigger an update in lifecourses index
            if (!pasInLifeCourses.ContainsKey(pa.Key)) return;

            foreach (string lcId in pasInLifeCourses[pa.Key])
            {
                updates.Add(new Tuple<string, BasePA>(lcId, pa));
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
        private static IEnumerable<BasePA> ReadSourcePAs(string basePath, Source source, string trsPath, Dictionary<string,List<string>> paFilter, bool usePaFilter)
        {
            Log.Debug($"Loading standardized PAs into memory from {Path.Combine(basePath, source.File_reference)}");
            
            var paDict = usePaFilter ? 
                new DataSet<StandardPA>(Path.Combine(basePath, source.File_reference)).Read().Where(x => paFilter.ContainsKey($"{source.Source_id}-{x.Pa_id}")).ToDictionary(x => x.Pa_id) : 
                new DataSet<StandardPA>(Path.Combine(basePath, source.File_reference)).Read().ToDictionary(x => x.Pa_id);
            
            if (paDict.Count == 0)
            {
                Log.Debug($"No standardized PAs matched the paFilter, skipping this source");
                yield break;
            }
            #region skip transcribed
            /*
            foreach (KeyValuePair<int, StandardPA> spa in paDict)
            {
                var pa = BasePA.Create(source, spa.Value, null);
                pa.InitKey();
                if (paFilter.Count > 0 && !paFilter.ContainsKey(pa.Key)) { continue; }
                yield return pa;
            }
            */
            #endregion
            

            #region use transcribed
            
            Log.Debug($"Reading transcribed PAs from {Path.Combine(trsPath, source.Original_data_reference)}");
            var trsSet = new DataSet<dynamic>(Path.Combine(trsPath, source.Original_data_reference));
            //Transcribed files can be pretty big so going over them row by row when matching to our standardised pa saves on memory.
            foreach (var transcribtion in trsSet.Read())
            {
                BasePA pa = null;
                try
                {
                    var trsPa = new TranscribedPA(transcribtion, source.Source_id);
                    if(usePaFilter && !paDict.ContainsKey(trsPa.Pa_id)) { continue; }
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
                if (usePaFilter && !paFilter.ContainsKey(pa.Key)) { continue; }

                yield return pa;
            }
            
            #endregion
        }
        private static IEnumerable<LifeCourse> ReadLifecoursesAndLinks(string basepath, int lifecourseCount = 0)
        { 
            Log.Info($"Reading lifecourses into memory from {Path.Combine(basepath, "life-courses", "life_courses.csv")}");
            var lifecoursesDataset = new DataSet<LifeCourse>(Path.Combine(basepath, "life-courses", "life_courses.csv"));
            
            if(DataVersion == null)
            {
                throw new Exception("DataVersion is not set, cannot continue.");
            }
            
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

            Log.Info($"Reading links from {Path.Combine(basepath, "links", "links.csv")}");
            var linksDataset = new DataSet<Link>(Path.Combine(basepath, "links", "links.csv"));
            var links = new List<Link>();
            foreach (var link in linksDataset.Read())
            {
                if (linkIdsInLifecourses.ContainsKey(link.Link_id))
                {
                    link.InitKey();
                    link.Data_version = DataVersion;
                    links.Add(link);
                }
            }
            Log.Info($"Loaded {links.Count} links");
            Log.Info($"Uniquefying links");
            var timer = Stopwatch.StartNew();
            var uniqueLinks = MakeLinksUnique(links);
            links.Clear();
            linkIdsInLifecourses.Clear();
            Log.Info($"Finished uniquefying links. Number of unique links: {uniqueLinks.Count}. Took: {timer.Elapsed}");
            timer.Stop();

            Log.Info($"Combining lifecourses and links");
            foreach (var lifecourse in lifecourses)
            {
                lifecourse.Links = new List<Link>();
                var linkIds = lifecourse.Link_ids.Split(',');
                var skipLifecourse = false;
                foreach (var id in linkIds)
                {
                    try
                    {
                        lifecourse.Links.Add(uniqueLinks[id]);
                    }
                    catch(KeyNotFoundException e)
                    {
                        // TODO: Temporary logic to prevent errors when lifecourses does not have any link ids. This is an exception
                        Log.Error($"A lifecourse points to a link id that does not exist in the unique links list. The lifecourse is skipped in indexation. Lifecourse id {lifecourse.Life_course_id}, link id {id}");
                        skipLifecourse = true;
                    }
                }
                if (skipLifecourse) continue;
                lifecourse.InitKey();
                lifecourse.Data_version = DataVersion;
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