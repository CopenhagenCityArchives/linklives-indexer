using Linklives.Domain;
using Linklives.Indexer.Domain;
using System;
using Nest;
using System.Collections;
using System.Collections.Generic;

namespace Linklives.Indexer.Lifecourses
{
    class Program
    {
        static void Main(string[] args)
        {
            var esClient = new ElasticClient(new ConnectionSettings(new Uri("ElasticSearch-URL"))
                .RequestTimeout(TimeSpan.FromMinutes(2))
                .DisableDirectStreaming());
        }
        private IEnumerable<BasePA> ReadPAs(string basePath)
        {
            var sources = new DataSet<Source>($"{basePath}\\auxilary_data\\sources\\sources.csv");
            foreach (var source in sources.Read())
            {
                var paSet = new DataSet<StandardPA>($"{basePath}\\{source.File_reference}");
                foreach (var pa in paSet.Read())
                {
                    yield return BasePA.Create(source.Type, pa);
                }
            }
        }
    }
}
