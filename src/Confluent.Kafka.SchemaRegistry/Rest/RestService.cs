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

using System.Collections.Generic;
using System.Net;
using System.Linq;
using System.Net.Http;
using System.IO;
using System;
using System.Threading.Tasks;
using Confluent.Kafka.SchemaRegistry.Rest.Entities.Requests;
using Confluent.Kafka.SchemaRegistry.Rest.Entities;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;


namespace Confluent.Kafka.SchemaRegistry.Rest
{
    /// <remarks>
    ///     It may be useful to expose this publically, but this is not
    ///     required by the Avro serializers, so we will keep this internal 
    ///     for now to minimize documentation / risk of API change etc.
    /// </remarks>
    internal class RestService : IRestService
    {
        private static readonly string acceptHeader = string.Join(", ", Versions.PreferredResponseTypes);

        /// <summary>
        ///     The index of the last client successfully used (or random if none worked).
        /// </summary>
        private int lastClientUsed;
        private object lastClientUsedLock = new object();

        /// <summary>
        ///     HttpClient instances corresponding to each provided schema registry Uri.
        /// </summary>
        private readonly List<HttpClient> clients;

        
        /// <summary>
        ///     Initializes a new instance of the RestService class.
        /// </summary>
        public RestService(string schemaRegistryUris, int timeoutMs)
        { 
            this.clients = schemaRegistryUris
                .Split(',')
                .Select(uri => uri.StartsWith("http", StringComparison.Ordinal) ? uri : "http://" + uri) // need http or https - use http if not present.
                .Select(uri => new HttpClient() { BaseAddress = new Uri(uri, UriKind.Absolute), Timeout = TimeSpan.FromMilliseconds(timeoutMs) })
                .ToList();
        }

        #region Base Requests

        private async Task<HttpResponseMessage> ExecuteOnOneInstanceAsync(HttpRequestMessage request)
        {
            // There may be many base urls - roll until one is found that works.
            //
            // Start with the last client that was used by this method, which only gets set on 
            // success, so it's probably going to work.
            //
            // Otherwise, try every client until a successful call is made (true even under 
            // concurrent access).

            string aggregatedErrorMessage = null;
            HttpResponseMessage response = null;
            bool gotOkAnswer = false;
            bool firstError = true;
            
            int startClientIndex;
            lock (lastClientUsedLock)
            {
                startClientIndex = this.lastClientUsed;
            }

            for (int i = startClientIndex; i < clients.Count + startClientIndex && !gotOkAnswer; ++i)
            {
                try
                {
                    response = await clients[startClientIndex % clients.Count]
                            .SendAsync(request).ConfigureAwait(false);

                    // In the case of an internal server error, try another server 
                    //   (reason could be e.g. "error while forwarding the request to the master")
                    // 4xx errors should not be retried (these are conclusive)
                    if (response.StatusCode == HttpStatusCode.InternalServerError)
                    {
                        if (!firstError)
                        {
                            aggregatedErrorMessage += "; ";
                        }
                        else 
                        {
                            firstError = false;
                        }

                        string message = "";
                        int errorCode = -1;
                        try
                        {
                            var errorObject = JObject.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
                            message = errorObject.Value<string>("message");
                            errorCode = errorObject.Value<int>("error_code");
                        }
                        catch
                        {
                            aggregatedErrorMessage += $"[{clients[i].BaseAddress}] {response.StatusCode}";
                        }

                        aggregatedErrorMessage += $"[{clients[i].BaseAddress}] {response.StatusCode} {errorCode} {message}";
                    }
                    else 
                    {
                        gotOkAnswer = true;
                    }
                }
                catch (HttpRequestException e)
                {
                    if (!firstError)
                    {
                        aggregatedErrorMessage += "; ";
                    }
                    else 
                    {
                        firstError = false;
                    }

                    aggregatedErrorMessage += $"[{clients[i].BaseAddress}] HttpRequestException: {e.Message}";
                }
            }

            if (!gotOkAnswer)
            {
                // All SR calls failed, return an exception containing information about each failure.
                // TODO: Allow non-empty aggregatedErrorMessage to be logged even when gotOkAnswer == true.
                throw new HttpRequestException(aggregatedErrorMessage);
            }

            if (!response.IsSuccessStatusCode)
            {
                // 4xx errors
                var errorObject = JObject.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
                var message = errorObject.Value<string>("message");
                var errorCode = errorObject.Value<int>("error_code");
                throw new SchemaRegistryException(message, response.StatusCode, errorCode);
            }

            // if we had success, set last client used so we start with this next time.
            lock (lastClientUsedLock)
            {
                this.lastClientUsed = (this.lastClientUsed + 1) % clients.Count;
            }

            return response;
        }


        /// <remarks>
        ///     Used for end points that return return a json object { ... }
        /// </remarks>
        private async Task<T> RequestAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var request = CreateRequest(endPoint, method, jsonBody);
            var response = await ExecuteOnOneInstanceAsync(request).ConfigureAwait(false);
            string responseJson = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            T t = JObject.Parse(responseJson).ToObject<T>();
            return t;
        }

        /// <remarks>
        ///     Used for end points that return a json array [ ... ]
        /// </remarks>
        private async Task<List<T>> RequestListOfAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var request = CreateRequest(endPoint, method, jsonBody);
            var response = await ExecuteOnOneInstanceAsync(request).ConfigureAwait(false);
            return JArray.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false)).ToObject<List<T>>();
        }

        private HttpRequestMessage CreateRequest(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            HttpRequestMessage request = new HttpRequestMessage(method, endPoint);
            request.Headers.Add("Accept", acceptHeader);
            if (jsonBody.Length != 0)
            {
                string stringContent = string.Join("\n", jsonBody.Select(x => JsonConvert.SerializeObject(x)));
                request.Content = new StringContent(stringContent, System.Text.Encoding.UTF8, Versions.SchemaRegistry_V1_JSON);
            }
            return request;
        }

        #endregion Base Requests

        #region Schemas

        public Task<SchemaString> GetSchemaAsync(int id)
            => RequestAsync<SchemaString>($"/schemas/ids/{id}", HttpMethod.Get);

        #endregion Schemas

        #region Subjects

        public Task<List<string>> GetSubjectsAsync()
            => RequestListOfAsync<string>("/subjects", HttpMethod.Get);

        public Task<List<string>> GetSubjectVersions(string subject)
            => RequestListOfAsync<string>($"/subjects/{subject}/versions", HttpMethod.Get);

        public Task<Schema> GetSchemaAsync(string subject, int version)
            => RequestAsync<Schema>($"/subjects/{subject}/versions/{version}", HttpMethod.Get);

        public Task<Schema> GetLatestSchemaAsync(string subject)
            => RequestAsync<Schema>($"/subjects/{subject}/versions/latest", HttpMethod.Get);

        public Task<SchemaId> PostSchemaAsync(string subject, string schema)
            => RequestAsync<SchemaId>($"/subjects/{subject}/versions", HttpMethod.Post, new SchemaString(schema));

        public Task<Schema> CheckSchemaAsync(string subject, string schema)
            => RequestAsync<Schema>($"/subjects/{subject}", HttpMethod.Post, new SchemaString(schema));

        #endregion Subjects

        #region Compatibility

        public Task<CompatibilityCheck> TestCompatibilityAsync(string subject, int versionId, string avroSchema)
            => RequestAsync<CompatibilityCheck>(
                    $"/compatibility/subjects/{subject}/versions/{versionId}",
                    HttpMethod.Post,
                    new SchemaString(avroSchema));

        public Task<CompatibilityCheck> TestLatestCompatibilityAsync(string subject, string avroSchema)
            => RequestAsync<CompatibilityCheck>(
                    $"/compatibility/subjects/{subject}/versions/latest",
                    HttpMethod.Post,
                    new SchemaString(avroSchema));

        #endregion Compatibility

        #region Config

        public Task<Config> GetGlobalCompatibilityAsync()
            => RequestAsync<Config>("/config", HttpMethod.Get);

        public Task<Config> GetCompatibilityAsync(string subject)
            => RequestAsync<Config>($"/config/{subject}", HttpMethod.Get);

        public Task<Config> PutGlobalCompatibilityAsync(Config.Compatbility compatibility)
            => RequestAsync<Config>("/config", HttpMethod.Put, new Config(compatibility));

        public Task<Config> PutCompatibilityAsync(string subject, Config.Compatbility compatibility)
            => RequestAsync<Config>($"/config/{subject}", HttpMethod.Put, new Config(compatibility));
            
        #endregion Config

        public void Dispose()
        {
            foreach (var client in this.clients)
            {
                client.Dispose();
            }    
        }
    }
}