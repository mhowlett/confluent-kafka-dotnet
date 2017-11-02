﻿// Copyright 2016-2017 Confluent Inc.
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

using System.Collections.Generic;
using System.Threading.Tasks;


namespace Confluent.Kafka.SchemaRegistry
{
    public interface ISchemaRegistryClient
    {
        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_RegisterAsync"]/*' />
        Task<int> RegisterAsync(string subject, string schema);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsync"]/*' />
        Task<string> GetSchemaAsync(int id);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsync_II"]/*' />
        Task<string> GetSchemaAsync(string subject, int version);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetLatestSchemaAsync"]/*' />
        Task<Schema> GetLatestSchemaAsync(string subject);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetAllSubjectsAsync"]/*' />
        Task<List<string>> GetAllSubjectsAsync();

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_IsCompatibleAsync"]/*' />
        Task<bool> IsCompatibleAsync(string subject, string avroSchema);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_ConstructRegistrySubject"]/*' />
        string ConstructSubjectName(string topic, bool isKey);


        // TODO: the following interfaces may be required.
        
        // Task<bool> CheckSchemaAsync(string subject, string schema);
        // Task<Config.Compatbility> GetGlobalCompatibility();
        // Task<Config.Compatbility> GetCompatibilityAsync(string subject);
        // Task<Config.Compatbility> PutGlobalCompatibilityAsync(Config.Compatbility compatibility);
        // Task<Config.Compatbility> UpdateCompatibilityAsync(string subject, Config.Compatbility compatibility);
    }
}
