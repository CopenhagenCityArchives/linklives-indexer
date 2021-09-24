using Linklives.Domain;
using Linklives.Indexer.Domain;
using NUnit.Framework;

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
            var dataset = new DataSet<StandardPA>(@"F:\link-lives\LL_data_v1.0.dev0\development\standardized_sources\CBP\CBP.csv");
            var pas = dataset.Read();
            Assert.IsNotNull(pas);
        }
    }
}