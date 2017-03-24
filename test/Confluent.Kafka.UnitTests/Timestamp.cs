// Copyright 2016-2017 Confluent Inc.
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

using System;
using Xunit;


namespace Confluent.Kafka.Tests
{
    public class TimestampTests
    {
        [Fact]
        public void Constuctor()
        {
            var ts = new Timestamp(new DateTime(2010, 3, 4), TimestampType.CreateTime);
            Assert.Equal(ts.DateTime, new DateTime(2010, 3, 4));
            Assert.Equal(ts.Type, TimestampType.CreateTime);
        }

        [Fact]
        public void Equality()
        {
            var ts1 = new Timestamp(new DateTime(2010, 3, 4), TimestampType.CreateTime);
            var ts2 = new Timestamp(new DateTime(2010, 3, 4), TimestampType.CreateTime);
            var ts3 = new Timestamp(new DateTime(2011, 3, 4), TimestampType.CreateTime);
            var ts4 = new Timestamp(new DateTime(2010, 3, 4), TimestampType.LogAppendTime);

            Assert.Equal(ts1, ts2);
            Assert.True(ts1.Equals(ts2));
            Assert.True(ts1 == ts2);
            Assert.False(ts1 != ts2);

            Assert.NotEqual(ts1, ts3);
            Assert.False(ts1.Equals(ts3));
            Assert.False(ts1 == ts3);
            Assert.True(ts1 != ts3);

            Assert.NotEqual(ts1, ts4);
            Assert.False(ts1.Equals(ts4));
            Assert.False(ts1 == ts4);
            Assert.True(ts1 != ts4);
        }

        [Fact]
        public void Conversion()
        {
            // check is to millisecond accuracy.
            var ts = new DateTime(2012, 5, 6, 12, 4, 3, 220, DateTimeKind.Utc);
            var unixTime = Timestamp.DateTimeToUnixTimestampMs(ts);
            var ts2 = Timestamp.UnixTimestampMsToDateTime(unixTime);
            Assert.Equal(ts, ts2);
            Assert.Equal(ts2.Kind, DateTimeKind.Utc);
        }

        [Fact]
        public void Rounding()
        {
            // check is to millisecond accuracy, rounding down the value
            var dateTimeAfterEpoch = new DateTime(2012, 5, 6, 12, 4, 3, 220, DateTimeKind.Utc);
            var afterEpoch1 = dateTimeAfterEpoch.AddTicks(1);
            var afterEpoch2 = dateTimeAfterEpoch.AddTicks(TimeSpan.TicksPerMillisecond - 1);
            var unixTimeAfterEpoch1 = Timestamp.DateTimeToUnixTimestampMs(afterEpoch1);
            var unixTimeAfterEpoch2 = Timestamp.DateTimeToUnixTimestampMs(afterEpoch2);
            var expectedUnixTimeAfterEpoch = Timestamp.DateTimeToUnixTimestampMs(dateTimeAfterEpoch);

            var dateTimeBeforeEpoch = new DateTime(1950, 5, 6, 12, 4, 3, 220, DateTimeKind.Utc);
            var beforeEpoch1 = dateTimeBeforeEpoch.AddTicks(1);
            var beforeEpoch2 = dateTimeBeforeEpoch.AddTicks(TimeSpan.TicksPerMillisecond - 1);
            var unixTimeBeforeEpoch1 = Timestamp.DateTimeToUnixTimestampMs(beforeEpoch1);
            var unixTimeBeforeEpoch2 = Timestamp.DateTimeToUnixTimestampMs(beforeEpoch2);
            var expectedUnixTimeBeforeEpoch = Timestamp.DateTimeToUnixTimestampMs(dateTimeBeforeEpoch);

            Assert.Equal(expectedUnixTimeAfterEpoch, unixTimeAfterEpoch1);
            Assert.Equal(expectedUnixTimeAfterEpoch, unixTimeAfterEpoch2);

            Assert.Equal(expectedUnixTimeBeforeEpoch, unixTimeBeforeEpoch1);
            Assert.Equal(expectedUnixTimeBeforeEpoch, unixTimeBeforeEpoch2);
        }
    }
}
