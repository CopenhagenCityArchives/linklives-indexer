using Linklives.Domain;
using Linklives.Indexer.Domain;
using NUnit.Framework;
using System.Linq;

namespace linklibes_indexer_test
{
    public class DataSetTests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Sandbox()
        {
            //var dataset = new DataSet<StandardPA>(@"F:\link-lives\LL_data_v1.0.dev0\development\standardized_sources\CBP\CBP.csv");
            //var pas = dataset.Read().Select(std => BasePA.Create(10, std)).ToList();
            //Assert.IsNotNull(pas[0]);
        }
    }
}