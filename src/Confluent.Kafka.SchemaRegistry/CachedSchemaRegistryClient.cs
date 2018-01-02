﻿// Copyright 2016-2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using Confluent.Kafka.SchemaRegistry.Rest;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System;


namespace Confluent.Kafka.SchemaRegistry
{
    /// <summary>
    ///     A Schema Registry client that caches request responses.
    /// </summary>
    public class CachedSchemaRegistryClient : ISchemaRegistryClient, IDisposable
    {
        private IRestService restService;
        private readonly int identityMapCapacity;
        private readonly Dictionary<int, string> schemaById = new Dictionary<int, string>();
        private readonly Dictionary<string /*subject*/, Dictionary<string, int>> idBySchemaBySubject = new Dictionary<string, Dictionary<string, int>>();
        private readonly Dictionary<string /*subject*/, Dictionary<int, string>> schemaByVersionBySubject = new Dictionary<string, Dictionary<int, string>>();
        
        /// <summary>
        ///     The default timeout value for Schema Registry REST API calls.
        /// </summary>
        public const int DefaultTimeout = 10000;

        /// <summary>
        ///     The default maximum capacity of the local schema cache.
        /// </summary>
        public const int DefaultMaxCapacity = 1024;

        /// <summary>
        ///     Initialize a new instance of the SchemaRegistryClient class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public CachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, object>> config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config properties must be specified.");
            }

            var schemaRegistryUrisMaybe = config.Where(prop => prop.Key.ToLower() == "schema.registry.urls").FirstOrDefault();
            if (schemaRegistryUrisMaybe.Value == null)
            {
                throw new ArgumentException("schema.registry.urls configuration property must be specified.");
            }
            var schemaRegistryUris = (string)schemaRegistryUrisMaybe.Value;

            var timeoutMsMaybe = config.Where(prop => prop.Key.ToLower() == "schema.registry.timeout.ms").FirstOrDefault();
            var timeoutMs = timeoutMsMaybe.Value == null ? DefaultTimeout : (int)timeoutMsMaybe.Value;

            var identityMapCapacityMaybe = config.Where(prop => prop.Key.ToLower() == "schema.registry.max.capacity").FirstOrDefault();
            this.identityMapCapacity = identityMapCapacityMaybe.Value == null ? DefaultMaxCapacity : (int)identityMapCapacityMaybe.Value;

            this.restService = new RestService(schemaRegistryUris, timeoutMs);
        }

        /// <remarks>
        ///     This is to make sure memory doesn't explode in the case of incorrect usage.
        /// 
        ///     It's behavior is pretty extreme - remove everything and start again if the 
        ///     cache gets full. However, in practical situations this is not expected.
        /// 
        ///     TODO: Implement an LRU Cache here or something instead (not high priority).
        /// </remarks>
        private bool CleanCacheIfFull()
        {
            if (
                this.schemaById.Count + 
                this.schemaByVersionBySubject.Sum(x => x.Value.Count) +
                this.idBySchemaBySubject.Sum(x => x.Value.Count)
                    >= identityMapCapacity)
            {
                // TODO: log if this happens.
                this.schemaById.Clear();
                this.idBySchemaBySubject.Clear();
                this.schemaByVersionBySubject.Clear();
                return true;
            }

            return false;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_RegisterAsync"]/*' />
        public async Task<int> RegisterAsync(string subject, string schema)
        {
            CleanCacheIfFull();
            
            if (!this.idBySchemaBySubject.TryGetValue(subject, out Dictionary<string, int> idBySchema))
            {
                idBySchema = new Dictionary<string, int>();
                this.idBySchemaBySubject[subject] = idBySchema;
            }

            if (!idBySchema.TryGetValue(schema, out int schemaId))
            {
                var registered = await restService.PostSchemaAsync(subject, schema).ConfigureAwait(false);
                schemaId = registered.Id;
                idBySchema[schema] = schemaId;
                schemaById[schemaId] = schema;
            }

            return schemaId;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsync"]/*' />
        public async Task<string> GetSchemaAsync(int id)
        {
            CleanCacheIfFull();

            if (!this.schemaById.TryGetValue(id, out string schema))
            {
                schema = (await restService.GetSchemaAsync(id).ConfigureAwait(false)).Schema;
                schemaById[id] = schema;
            }

            return schema;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsyncSubjectVersion"]/*' />
        public async Task<string> GetSchemaAsync(string subject, int version)
        {
            CleanCacheIfFull();

            if (!schemaByVersionBySubject.TryGetValue(subject, out Dictionary<int, string> schemaByVersion))
            {
                schemaByVersion = new Dictionary<int, string>();
                schemaByVersionBySubject[subject] = schemaByVersion;
            }

            if (!schemaByVersion.TryGetValue(version, out string schemaString))
            {
                var schema = await restService.GetSchemaAsync(subject, version).ConfigureAwait(false);
                schemaString = schema.SchemaString;
                schemaByVersion[version] = schemaString;
                schemaById[schema.Id] = schemaString;
            }

            return schemaString;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetLatestSchemaAsync"]/*' />
        public async Task<Schema> GetLatestSchemaAsync(string subject)
            => await restService.GetLatestSchemaAsync(subject).ConfigureAwait(false);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetAllSubjectsAsync"]/*' />
        public Task<List<string>> GetAllSubjectsAsync()
            => restService.GetSubjectsAsync();

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_IsCompatibleAsync"]/*' />
        public async Task<bool> IsCompatibleAsync(string subject, string schemaString)
            => (await restService.TestLatestCompatibilityAsync(subject, schemaString).ConfigureAwait(false)).IsCompatible;

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_ConstructKeySubjectName"]/*' />
        public string ConstructKeySubjectName(string topic)
            => $"{topic}-key";

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_ConstructValueSubjectName"]/*' />
        public string ConstructValueSubjectName(string topic)
            => $"{topic}-value";

        public void Dispose()
            => restService.Dispose();
    }
}