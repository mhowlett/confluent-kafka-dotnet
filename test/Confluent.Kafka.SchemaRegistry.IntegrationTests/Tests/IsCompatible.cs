using System;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.SchemaRegistry;

namespace Confluent.Kafka.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void IsCompatible(string server)
        {
            var topicName = Guid.NewGuid().ToString();

            var testSchema1 = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var sr = new CachedSchemaRegistryClient(new Dictionary<string, object>{ { "schema.registry.urls", server } });

            var subject = sr.ConstructKeySubjectName(topicName);
            var id = sr.RegisterAsync(subject, testSchema1).Result;

            var testSchema2 = // incompatible with testSchema1
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_shape\",\"type\":[\"string\",\"null\"]}]}";

            Assert.False(sr.IsCompatibleAsync(subject, testSchema2).Result);
            Assert.True(sr.IsCompatibleAsync(subject, testSchema1).Result);

            // Note: backwards / forwards compatibility scenarios are not tested here. This is really just testing the API call.
        }
    }
}
