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
using System.Runtime.Serialization;
using System.Collections.Generic;


namespace  Confluent.SchemaRegistry
{
    /// <summary>
    ///     Represents a schema.
    /// </summary>
    [DataContract]
    public class Schema
    {
        /// <summary>
        ///     A string representation of the schema.
        /// </summary>
        [DataMember(Name = "schema")]
        public string SchemaString { get; set; }

        /// <summary>
        ///     A list of schemas referenced by this schema.
        /// </summary>
        [DataMember(Name = "references")]
        public List<SchemaReference> References { get; set; }

        /// <summary>
        ///     The type of schema: AVRO, PROTOBUF, JSON
        /// </summary>
        [DataMember(Name = "schemaType")]
        public SchemaType SchemaType { get; set; }

        /// <summary>
        ///     Empty constructor for serialization
        /// </summary>
        private Schema() { }

        /// <summary>
        ///     Initializes a new instance of this class.
        /// </summary>
        /// <param name="schemaString">
        ///     String representation of the schema.
        /// </param>
        /// <param name="schemaType">
        ///     The schema type: AVRO, PROTOBUF, JSON
        /// </param>
        /// <param name="references">
        ///     A list of schemas referenced by this schema.
        /// </param>
        public Schema(string schemaString, List<SchemaReference> references, SchemaType schemaType)
        {
            SchemaString = schemaString;
            References = references;
            SchemaType = schemaType;
        }

        /// <summary>
        ///     Determines whether this instance and a specified object, which must also be an
        ///     instance of this type, have the same value (Overrides Object.Equals(Object))
        /// </summary>
        /// <param name="obj">
        ///     The instance to compare to this instance.
        /// </param>
        /// <returns>
        ///     true if obj is of the required type and its value is the same as this instance;
        ///     otherwise, false. If obj is null, the method returns false.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            Schema that = (Schema)obj;
            return Equals(that);
        }

        /// <summary>
        ///     Determines whether this instance and another specified object of the same type are
        ///     the same.
        /// </summary>
        /// <param name="other">
        ///     The instance to compare to this instance.
        /// </param>
        /// <returns>
        ///     true if the value of the other parameter is the same as the value of this instance; 
        ///     otherwise, false. If other is null, the method returns false.
        /// </returns>
        public bool Equals(Schema other)
            => this.SchemaString == other.SchemaString;

        /// <summary>
        ///     Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this instance.
        /// </returns>
        /// <remarks>
        ///     The hash code returned is that of the Schema property,
        ///     since the other properties are effectively derivatives
        ///     of this property.
        /// </remarks>
        public override int GetHashCode()
        {
            return SchemaString.GetHashCode();
        }

        /// <summary>
        ///     Compares this instance with another instance of this object type and indicates whether
        ///     this instance precedes, follows, or appears in the same position in the sort order
        ///     as the specified schema reference.
        /// </summary>
        /// <param name="other">
        ///     The instance to compare with this instance.
        /// </param>
        /// <returns>
        ///     A 32-bit signed integer that indicates whether this instance precedes, follows, or
        ///     appears in the same position in the sort order as the other parameter. Less than 
        ///     zero: this instance precedes other. Zero: this instance has the same position in
        ///     the sort order as other. Greater than zero: This instance follows other OR other 
        ///     is null.
        /// </returns>
        /// <remarks>
        ///     This method considers only the Schema property, since the other two properties are
        ///     effectively derivatives of this property.
        /// </remarks>
        public int CompareTo(Schema other)
        {
            if (other == null)
            {
                throw new ArgumentException("Cannot compare object of type UnregisteredSchema with null.");
            }

            return SchemaString.CompareTo(other.SchemaString);

            // If the schema strings are equal and any of the other properties are not,
            // then this is a logical error. Assume that this prevented/handled elsewhere.
        }

        /// <summary>
        ///     Returns a summary string representation of the object.
        /// </summary>
        /// <returns>
        ///     A string that represents the object.
        /// </returns>
        public override string ToString()
            => $"{{chars={SchemaString.Length}, type={SchemaType}, references={References.Count}}}";

    }
}
