using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Search;

namespace SearchMigration
{
    static class Settings
    {
        internal static string LegacySearchServiceName = ConfigurationManager.AppSettings["LegacySearchServiceName"];
        internal static string LegacySearchServiceApiKey = ConfigurationManager.AppSettings["LegacySearchServiceApiKey"];
        internal static string LegacySearchIndex = ConfigurationManager.AppSettings["LegacySearchIndex"];
        internal static string TargetSearchServiceName = ConfigurationManager.AppSettings["TargetSearchServiceName"];
        internal static string TargetSearchServiceApiKey = ConfigurationManager.AppSettings["TargetSearchServiceApiKey"];
        internal static string TargetSearchIndex = ConfigurationManager.AppSettings["TargetSearchIndex"];
    }

    class Migratoratron
    {
        bool DO_NOT_COMMIT_INDEX = false;  // prevents the target index from being created
        bool DO_NOT_COMMIT_DOCUMENTS = false;  // prevents the target docs from being uploaded
        bool FULL_TRACE = false;  // just outputs the doc titles

        // References to source and target search services
        ISearchServiceClient _legacySearchClient = null;
        ISearchServiceClient _targetSearchClient = null;
        ISearchIndexClient _legacyIndexClient = null;
        ISearchIndexClient _targetIndexClient = null;

        const string KEY_FIELD = "Key";
        const string TITLE_FIELD = "Title";
        const int PAGE_SIZE = 50;
        const int FINAL_CHECK_INDEXING_TIMEOUT_SECONDS = 5;

        static internal void Run()
        {
            // Initialize
            Migratoratron migrate = new Migratoratron();
            migrate.Init();

            // transfer the old schema over
            migrate.TransferSchema();

            // transfer the old documents over
            migrate.TransferDocuments();
        }

        void Init()
        {
            // References to source and target search services
            _legacySearchClient = new SearchServiceClient(Settings.LegacySearchServiceName, new SearchCredentials(Settings.LegacySearchServiceApiKey));
            _targetSearchClient = new SearchServiceClient(Settings.TargetSearchServiceName, new SearchCredentials(Settings.TargetSearchServiceApiKey));
        }

        void TransferDocuments()
        {
            // set up the indexes
            _legacyIndexClient = new SearchIndexClient(Settings.LegacySearchServiceName, Settings.LegacySearchIndex, _legacySearchClient.Credentials);
            _targetIndexClient = new SearchIndexClient(Settings.TargetSearchServiceName, Settings.TargetSearchIndex, _targetSearchClient.Credentials);
            long totaldocs = _legacyIndexClient.Documents.Count().DocumentCount;

            Console.WriteLine(String.Format("Found {0} documents to transfer", totaldocs.ToString()));
            Console.WriteLine("Indexing");

            // create an indexer to put the new docs in for us
            var indexer = new List<Microsoft.Azure.Search.Models.IndexAction>();

            // now get all the docs, enumerate and push into new index
            // Liam tells us that things could go wrong if docs are added during this => pause indexing.
            // We get FULL docs back in groups of 50, by default, but i guess this could change - be good to have a queryable "safe" number 

            // store anything that screws up
            List<string> failures = new List<string>();

            // get groups of PAGE_SIZE and push
            int maxpages = (int)(totaldocs / PAGE_SIZE) + 1;          
            for (int i = 0; i < maxpages; i++)
            {
                Console.Write("*");

                // search config as it brings back 50 at a time
                Microsoft.Azure.Search.Models.SearchParameters sparams = new Microsoft.Azure.Search.Models.SearchParameters();
                sparams.Top = PAGE_SIZE;
                sparams.Skip = i * PAGE_SIZE;

                // get all in that page
                var documents = _legacyIndexClient.Documents.Search("*", sparams).Results;

                // get the old docs transposed
                foreach (var olddoc in documents)
                {
                    var document = new Microsoft.Azure.Search.Models.Document();
                    foreach (var key in olddoc.Document.Keys)
                    {
                        object value;
                        if (olddoc.Document.TryGetValue(key, out value))
                        {
                            document.Add(key, value);
                        }
                        else
                        {
                            failures.Add(key);
                        }
                    }

                    if (FULL_TRACE) Console.WriteLine(String.Format("Indexed {0} ({1})", document[TITLE_FIELD], document[KEY_FIELD]));

                    // now add to the indexer as a new item
                    indexer.Add(new Microsoft.Azure.Search.Models.IndexAction(
                        Microsoft.Azure.Search.Models.IndexActionType.Upload, document));
                }

                if (!DO_NOT_COMMIT_DOCUMENTS)
                {
                    //now get the indexer to batch import
                    _targetIndexClient.Documents.Index(new Microsoft.Azure.Search.Models.IndexBatch(indexer));
                }                

                // reset and go again
                indexer.Clear();

            }

            Console.WriteLine(String.Empty);
            Console.WriteLine(String.Format("Done. Short delay to let the indexing complete <queue music>"));

            // set a timeout so the indexing can complete before we check the count
            System.Threading.Thread.Sleep(FINAL_CHECK_INDEXING_TIMEOUT_SECONDS * 1000);

            // were all documents indexed?
            if (totaldocs == _targetIndexClient.Documents.Count().DocumentCount)
            {
                Console.WriteLine(String.Format("ALL DOCUMENTS INDEXED! Found {0} documents in the new index.", _targetIndexClient.Documents.Count().DocumentCount.ToString()));
            }
            else
            {
                Console.WriteLine(String.Format("Found {0} documents in the new index", _targetIndexClient.Documents.Count().DocumentCount.ToString()));

                if (failures.Count > 0)
                {
                    Console.WriteLine("The following were not indexed:");
                    foreach (var item in failures)
                    {
                        Console.WriteLine(item);
                    }
                }
            }

        }
        
        void TransferSchema()
        {
            if (DO_NOT_COMMIT_INDEX) return;

            Console.WriteLine("Started copying old index accross.");

            try
            {
                // Get the old schema
                var legacyIndex = _legacySearchClient.Indexes.Get(Settings.LegacySearchIndex).Index;

                // delete the target index if it exists
                if (_targetSearchClient.Indexes.Exists(Settings.TargetSearchIndex))
                {
                    _targetSearchClient.Indexes.Delete(Settings.TargetSearchIndex);
                }

                // the new index - could prob just copy them directly, but may need some tweaks
                List<Microsoft.Azure.Search.Models.Field> fields = new List<Microsoft.Azure.Search.Models.Field>();

                // enumerate legacy and push to target
                foreach (var lfield in legacyIndex.Fields)
                {
                    fields.Add(new Microsoft.Azure.Search.Models.Field() {
                        Name = lfield.Name,
                        Type = lfield.Type,
                        IsKey = lfield.IsKey,
                        IsSearchable = lfield.IsSearchable,
                        IsFilterable = lfield.IsFilterable,
                        IsSortable = lfield.IsSortable,
                        IsFacetable = lfield.IsFacetable,
                        IsRetrievable = lfield.IsRetrievable
                    });
                }

                // the next index
                var index = new Microsoft.Azure.Search.Models.Index(Settings.TargetSearchIndex, fields);

                // create the index
                _targetSearchClient.Indexes.Create(index);

                Console.WriteLine("Successfully copied index!");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Problem creating the index: {0}\r\n", ex.Message.ToString());
            }
        }
    }
}
