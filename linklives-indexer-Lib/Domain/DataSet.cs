using CsvHelper;
using CsvHelper.Configuration;
using Linklives.Domain;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

namespace Linklives.Indexer.Domain
{
    public class DataSet<T>
    {
        public string Path { get; private set; }

        public DataSet(string path)
        {
            Path = path;
        }
        public IEnumerable<T> Read()
        {
            using (var reader = new StreamReader(Path))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();
                csv.ReadHeader();
                while (csv.Read())
                {
                    var record = csv.GetRecord<T>();
                    if (record is KeyedItem)
                    {
                        (record as KeyedItem).InitKey();
                    }
                    yield return record;
                }
            }
        }
    }
}
