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
        public string FileName { get => new FileInfo(Path).Name; }

        public DataSet(string path)
        {
            Path = path;
        }
        public IEnumerable<T> Read(bool initKeys = false)
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                MissingFieldFound = null, //Set to null so we ignore properties that dont have a matching column in the CSV
                HeaderValidated = null, //Skip header validation since that would also throw an error if a property didnt have a matching column
                Delimiter = ","
            };
            using (var reader = new StreamReader(Path))
            using (var csv = new CsvReader(reader, config))
            {
                csv.Read();
                csv.ReadHeader();
                while (csv.Read())
                {
                    var record = csv.GetRecord<T>();
                    if (initKeys && record is KeyedItem)
                    {
                        (record as KeyedItem).InitKey();
                    }
                    yield return record;
                }
            }
        }
    }
}
