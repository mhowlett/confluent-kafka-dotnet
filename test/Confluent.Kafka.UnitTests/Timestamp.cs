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
        public void ConstuctorWithDateTime()
        {
            var ts = new Timestamp(new DateTime(2010, 3, 4), TimestampType.CreateTime);
            Assert.Equal(new DateTime(2010, 3, 4).ToUniversalTime(), ts.DateTime);
            Assert.Equal(Timestamp.DateTimeToUnixTimestampMs(new DateTime(2010, 3, 4).ToUniversalTime()), ts.UnixTimestampMs);
            Assert.Equal(TimestampType.CreateTime, ts.Type);
        }


        [Fact]
        public void ConstuctorWithTimestamp()
        {
            var ts = new Timestamp(123456789, TimestampType.CreateTime);
            Assert.Equal(123456789, ts.UnixTimestampMs);
            Assert.Equal(Timestamp.UnixTimestampMsToDateTime(123456789), ts.DateTime);
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
            Assert.Equal(1336305843220, unixTime);
            Assert.Equal(ts, ts2);
            Assert.Equal(ts2.Kind, DateTimeKind.Utc);
        }

        [Fact]
        public void Rounding()
        {
            // check is to millisecond accuracy, rounding down the value
            
            var dateTimeAfterEpoch = new DateTime(2012, 5, 6, 12, 4, 3, 220, DateTimeKind.Utc);
            var dateTimeBeforeEpoch = new DateTime(1950, 5, 6, 12, 4, 3, 220, DateTimeKind.Utc);

            foreach (var datetime in new[] { dateTimeAfterEpoch, dateTimeBeforeEpoch })
            {

                var unixTime1 = Timestamp.DateTimeToUnixTimestampMs(datetime.AddTicks(1));
                var unixTime2 = Timestamp.DateTimeToUnixTimestampMs(datetime.AddTicks(TimeSpan.TicksPerMillisecond - 1));
                var unixTime3 = Timestamp.DateTimeToUnixTimestampMs(datetime.AddTicks(TimeSpan.TicksPerMillisecond));
                var unixTime4 = Timestamp.DateTimeToUnixTimestampMs(datetime.AddTicks(-1));

                var expectedUnixTime = Timestamp.DateTimeToUnixTimestampMs(datetime);
                
                Assert.Equal(expectedUnixTime, unixTime1);
                Assert.Equal(expectedUnixTime, unixTime2);
                Assert.Equal(expectedUnixTime + 1, unixTime3);
                Assert.Equal(expectedUnixTime - 1, unixTime4);
            }
        }
    }
}
