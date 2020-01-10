// Copyright 2020 Confluent Inc.
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

using Moq;
using Xunit;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using System;
using Confluent.Kafka;
using Confluent.Kafka.Examples.AvroSpecific;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class ProtobufSerializeDeserialzeTests
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private string testTopic;
        private Dictionary<string, int> store = new Dictionary<string, int>();

        public ProtobufSerializeDeserialzeTests()
        {
            testTopic = "topic";
            var schemaRegistryMock = new Mock<ISchemaRegistryClient>();
            schemaRegistryMock.Setup(x => x.ConstructValueSubjectName(testTopic, It.IsAny<string>())).Returns($"{testTopic}-value");
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync("topic-value", It.IsAny<string>())).ReturnsAsync(
                (string topic, string schema) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>())).ReturnsAsync((int id) => store.Where(x => x.Value == id).First().Key);
            schemaRegistryClient = schemaRegistryMock.Object;   
        }


        [Fact]
        public void IntSerDe()
        {
            var avroSerializer = new ProtobufSerializer<UInt32Value>(schemaRegistryClient);

            var v = new UInt32Value();
            v.Value = 42;
            var m = v as IMessage<UInt32Value>;
            foreach (var f in m.Descriptor.Fields.InDeclarationOrder())
            {
                Console.WriteLine(f.ToString());
            }
            
            // var avroDeserializer = new Deserializer<int>(schemaRegistryClient);
            // byte[] bytes;
            // bytes = avroSerializer.SerializeAsync(1234, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            // Assert.Equal(1234, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }
    }
}