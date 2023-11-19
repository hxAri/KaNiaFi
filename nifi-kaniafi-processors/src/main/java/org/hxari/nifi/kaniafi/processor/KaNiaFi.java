/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hxari.nifi.kaniafi.processor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KaNiaFi {

    final public static String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    final public static String DATETIME_TIMEZONE = "Asia/Tokyo";
    
    final public static ObjectMapper objectMapper = new ObjectMapper();

    public static String normalizeUnixTimestamp( double unixtime ) {
        return KaNiaFi.normalizeUnixTimestamp( unixtime, DATETIME_FORMAT, DATETIME_TIMEZONE );
    }

    public static String normalizeUnixTimestamp( double unixtime, String format ) {
        return KaNiaFi.normalizeUnixTimestamp( unixtime, format, DATETIME_TIMEZONE );
    }

    public static String normalizeUnixTimestamp( double unixtime, String format, String timezone ) {
		LocalDateTime dateTime = LocalDateTime.ofInstant( 
			Instant.ofEpochSecond( ( long ) unixtime ), 
			ZoneId.of( timezone )
		);
		return dateTime.format( DateTimeFormatter.ofPattern( format ) );
    }

}
