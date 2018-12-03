// Copyright 2018 Confluent Inc.
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Text;


namespace Confluent.Kafka
{
    public interface IDeserializer<T>
    {
        /// <summary>
        ///     Deserialize a message key or value.
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="messageAncillary">
        ///     Properties of the message the data is associated with in
        ///     addition to the key or value.
        /// </param>
        /// <param name="source">
        ///     The TopicPartition from which the message was consumed.
        /// </param>
        /// <param name="isKey">
        ///     True if deserializing the message key, false if deserializing the
        ///     message value.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        T Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source);
    }


    /// <summary>
    ///     Deserializers that can be used with <see cref="Confluent.Kafka.Consumer" />.
    /// </summary>
    public static class Deserializers
    {
        private static readonly Dictionary<Type, object> deserializers = new Dictionary<Type, object>
        {
            {typeof(string), new Utf8Deserializer() },
            {typeof(Null), new NullDeserializer() },
            {typeof(Ignore), new IgnoreDeserializer() },
            {typeof(long), new LongDeserializer() },
            {typeof(int), new Int32Deserializer() },
            {typeof(float), new FloatDeserializer() },
            {typeof(double), new DoubleDeserializer() },
            {typeof(byte[]), new ByteArrayDeserializer() },
        };
        
        public static IDeserializer<T> GetBuiltIn<T>()
        {
            if (deserializers.TryGetValue(typeof(T), out var deserializer))
                return (IDeserializer<T>)deserializer;

            throw new ArgumentException($"No Deserializer available for type: {typeof(T).Name}");
        }

        public static bool TryGetBuiltIn<T>(out IDeserializer<T> deserializer)
        {
            if(deserializers.TryGetValue(typeof(T), out var d))
            {
                deserializer = (IDeserializer<T>)d;
                return true;
            }

            deserializer = null;
            return false;
        }

        /// <summary>
        ///     Deserializes a UTF8 encoded string.
        /// </summary>
        public static readonly IDeserializer<string> UTF8 = new Utf8Deserializer();

        /// <summary>
        ///     Deserializes a null value to a null value.
        /// </summary>
        public static readonly IDeserializer<Null> Null = new NullDeserializer();

        /// <summary>
        ///     'Deserializes' any value to a null value.
        /// </summary>
        public static readonly IDeserializer<Ignore> Ignore = new IgnoreDeserializer();

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) <see cref="System.Int64"/> value from a byte array.
        /// </summary>
        /// <returns>
        ///     The deserialized <see cref="System.Int64"/> value.
        /// </returns>
        public static readonly IDeserializer<long> Long = new LongDeserializer();

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) <see cref="System.Int32"/> value from a byte array.
        /// </summary>
        /// <returns>
        ///     The deserialized <see cref="System.Int32"/> value.
        /// </returns>
        public static readonly IDeserializer<int> Int32 = new Int32Deserializer();

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) System.Single value from a byte array.
        /// </summary>
        /// <returns>
        ///     The deserialized System.Single value.
        /// </returns>
        public static readonly IDeserializer<float> Float = new FloatDeserializer();

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) System.Double value from a byte array.
        /// </summary>
        /// <returns>
        ///     The deserialized System.Double value.
        /// </returns>
        public static readonly IDeserializer<double> Double = new DoubleDeserializer();

        /// <summary>
        ///     Deserializes a System.Byte[] value (or null).
        /// </summary>
        public static readonly IDeserializer<byte[]> ByteArray = new ByteArrayDeserializer();


        private class Utf8Deserializer : IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source)
            {
                if (isNull)
                {
                    return null;
                }

                try
                {
#if NETCOREAPP2_1
                    return Encoding.UTF8.GetString(data);
#else
                    return Encoding.UTF8.GetString(data.ToArray());
#endif
                }
                catch (Exception e)
                {
                    throw new DeserializationException("Error occured deserializing UTF8 string value", e);
                }
            }
        }
        private class NullDeserializer : IDeserializer<Null>
        {
            public Null Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source)
            {
                if (!isNull)
                {
                    throw new DeserializationException("Deserializer<Null> may only be used to deserialize data that is null.");
                }

                return null;
            }
        }
        private class IgnoreDeserializer : IDeserializer<Ignore>
        {
            public Ignore Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source)
            {
                return null;
            }
        }
        private class Int32Deserializer : IDeserializer<int>
        {
            public int Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source)
            {
                if (isNull)
                {
                    throw new DeserializationException($"Null data encountered deserializing an Int32 value");
                }

                if (data.Length != 4)
                {
                    throw new DeserializationException($"Deserializer<Int32> encountered data of length {data.Length}. Expecting data length to be 4.");
                }

                // network byte order -> big endian -> most significant byte in the smallest address.
                return
                    (((int)data[0]) << 24) |
                    (((int)data[1]) << 16) |
                    (((int)data[2]) << 8) |
                    (int)data[3];
            }
        }
        private class LongDeserializer : IDeserializer<long>
        {
            public long Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source)
            {
                if (isNull)
                {
                    throw new DeserializationException($"Null data encountered deserializing Int64 value.");
                }

                if (data.Length != 8)
                {
                    throw new DeserializationException($"Deserializer<Long> encountered data of length {data.Length}. Expecting data length to be 8.");
                }

                // network byte order -> big endian -> most significant byte in the smallest address.
                long result = ((long)data[0]) << 56 |
                    ((long)(data[1])) << 48 |
                    ((long)(data[2])) << 40 |
                    ((long)(data[3])) << 32 |
                    ((long)(data[4])) << 24 |
                    ((long)(data[5])) << 16 |
                    ((long)(data[6])) << 8 |
                    (data[7]);
                return result;

            }
        }
        private class FloatDeserializer : IDeserializer<float>
        {
            public float Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source)
            {
                if (isNull)
                {
                    throw new DeserializationException($"Null data encountered deserializing an float value.");
                }

                if (data.Length != 4)
                {
                    throw new DeserializationException($"Deserializer<float> encountered data of length {data.Length}. Expecting data length to be 4.");
                }

                // network byte order -> big endian -> most significant byte in the smallest address.
                if (BitConverter.IsLittleEndian)
                {
                    unsafe
                    {
                        float result = default(float);
                        byte* p = (byte*)(&result);
                        *p++ = data[3];
                        *p++ = data[2];
                        *p++ = data[1];
                        *p++ = data[0];
                        return result;
                    }
                }
                else
                {
                    try
                    {
#if NETCOREAPP2_1
                        return BitConverter.ToSingle(data);
#else
                        return BitConverter.ToSingle(data.ToArray(), 0);
#endif
                    }
                    catch (Exception e)
                    {
                        throw new DeserializationException("Error occured deserializing float value.", e);
                    }
                }
            }
        }
        private class DoubleDeserializer : IDeserializer<double>
        {
            public double Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source)
            {
                if (isNull)
                {
                    throw new DeserializationException($"Null data encountered deserializing an double value.");
                }

                if (data.Length != 8)
                {
                    throw new DeserializationException($"Deserializer<double> encountered data of length {data.Length}. Expecting data length to be 8.");
                }

                // network byte order -> big endian -> most significant byte in the smallest address.
                if (BitConverter.IsLittleEndian)
                {
                    unsafe
                    {
                        double result = default(double);
                        byte* p = (byte*)(&result);
                        *p++ = data[7];
                        *p++ = data[6];
                        *p++ = data[5];
                        *p++ = data[4];
                        *p++ = data[3];
                        *p++ = data[2];
                        *p++ = data[1];
                        *p++ = data[0];
                        return result;
                    }
                }
                else
                {
                    try
                    {
#if NETCOREAPP2_1
                                    return BitConverter.ToDouble(data);
#else
                        return BitConverter.ToDouble(data.ToArray(), 0);
#endif
                    }
                    catch (Exception e)
                    {
                        throw new DeserializationException("Error occured deserializing double value.", e);
                    }
                }
            }
        }
        private class ByteArrayDeserializer : IDeserializer<byte[]>
        {
            public byte[] Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageAncillary messageAncillary, TopicPartition source)
            {
                if (isNull) { return null; }
                return data.ToArray();
            }
        }
    }
}
