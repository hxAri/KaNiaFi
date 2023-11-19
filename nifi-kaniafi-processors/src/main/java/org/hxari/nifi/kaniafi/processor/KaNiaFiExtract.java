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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "extract", "instagram", "kanashī", "kaniafi", "request", "response" })
@CapabilityDescription( "Extracting the results of Kanashī requests, as well as validating the FlowFIle content" )
@SeeAlso({ KaNiaFiExtractUser.class })
@ReadsAttributes({ @ReadsAttribute( attribute="x", description="X" ) })
@WritesAttributes({ @WritesAttribute( attribute="y", description="Y" ) })
public class KaNiaFiExtract extends AbstractProcessor {

	final public static PropertyDescriptor ALLOW_SET_ATTRIBUTE_PROPERTY = new PropertyDescriptor.Builder()
			.name( "allow.set.attribute" )
			.displayName( "Allow Set Attributes" )
			.description( "Allow the processor to set attributes automatically." )
			.required( false )
			.defaultValue( "true" )
			.addValidator( StandardValidators.BOOLEAN_VALIDATOR )
			.build();
	
	final public static PropertyDescriptor CHARSET_PROPERTY = new PropertyDescriptor.Builder()
			.name( "charset" )
			.displayName( "Character Set" )
			.description( "Specify Charset of FlowFile content when transfering into another processor." )
			.required( false )
			.defaultValue( "UTF-8" )
			.addValidator( StandardValidators.CHARACTER_SET_VALIDATOR )
			.build();
	
	final public static PropertyDescriptor DATETIME_FORMAT_PROPERTY = new PropertyDescriptor.Builder()
			.name( "datetime.format" )
			.displayName( "Datetime Format" )
			.description( "Convert Unix Timestamp to Datetime format." )
			.required( false )
			.defaultValue( KaNiaFi.DATETIME_FORMAT )
			.addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
			.build();
	
	final public static PropertyDescriptor TIMEZONE_ID_PROPERTY = new PropertyDescriptor.Builder()
			.name( "timezone.id" )
			.displayName( "Timezone Id" )
			.description( "Adjust datetime to time zone." )
			.required( false )
			.defaultValue( KaNiaFi.DATETIME_TIMEZONE )
			.addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
			.build();

	final public static Relationship CHECKPOINT_RELATIONSHIP = new Relationship.Builder()
			.name( "checkpoint" )
			.description( "Checkpointed request." )
			.autoTerminateDefault( true )
			.build();
	
	final public static Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
			.name( "failure" )
			.description( "Failed parse flowfile content." )
			.build();
	
	final public static Relationship INVALID_RELATIONSHIP = new Relationship.Builder()
			.name( "invalid" )
			.description( "Invalid request url." )
			.build();
	
	final public static Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
			.name( "success" )
			.description( "The results of the request sent and the response received match." )
			.build();
	
	final public static Relationship UNAUTHORIZED_RELATIONSHIP = new Relationship.Builder()
			.name( "unauthorized" )
			.description( "The request was not authenticated." )
			.autoTerminateDefault( true )
			.build();
	
	final public static Relationship UNPARSED_RELATIONSHIP = new Relationship.Builder()
			.name( "unparsed" )
			.description( "Failed to parse request response." )
			.autoTerminateDefault( true )
			.build();
	
	private static final String URL_PATTERN = "^(?:https\\:\\/\\/)?(?:(?:[a-zA-Z]+(?:[a-zA-Z0-9\\-\\.]*[a-zA-Z0-9]))\\.)?instagram\\.com\\/?(?:[^\n]*)?$";
	private static final String UNAUTHORIZED_PATTERN = "^\\<Response\s+\\[401\\]\\>$";

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	@Override
	protected void init( final ProcessorInitializationContext context ) {
		
		descriptors = new ArrayList<>();
		descriptors.add( ALLOW_SET_ATTRIBUTE_PROPERTY );
		descriptors.add( CHARSET_PROPERTY );
		descriptors.add( DATETIME_FORMAT_PROPERTY );
		descriptors.add( TIMEZONE_ID_PROPERTY );
		descriptors = Collections.unmodifiableList( descriptors );
		
		relationships = new HashSet<>();
		relationships.add( CHECKPOINT_RELATIONSHIP );
		relationships.add( FAILURE_RELATIONSHIP );
		relationships.add( INVALID_RELATIONSHIP );
		relationships.add( SUCCESS_RELATIONSHIP );
		relationships.add( UNAUTHORIZED_RELATIONSHIP );
		relationships.add( UNPARSED_RELATIONSHIP );
		relationships = Collections.unmodifiableSet( relationships );
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled( final ProcessContext context ) {
	}

	@Override
	public void onTrigger( final ProcessContext context, final ProcessSession session ) {
		FlowFile flowFile = session.get();
		if( flowFile != null ) {
			ComponentLog logger = getLogger();
			Relationship relationship = null;
			ObjectMapper objectMapper = new ObjectMapper();
			InputStream inputStream = session.read( flowFile );
			Map<String, String> attributes = new LinkedHashMap<>();
			try {
				JsonNode rootNode = objectMapper.readTree( inputStream );
				JsonNode requestNode = rootNode.get( "request" );
				JsonNode responseNode = rootNode.get( "response" );
				JsonNode contentNode = responseNode.get( "content" );
				String content = contentNode.toString();
				JsonNode targetNode = rootNode.get( "target" );
				String target = targetNode.asText();
				if( target.matches( URL_PATTERN ) ) {
					JsonNode statusNode = responseNode.get( "status" );
					if( statusNode.asText().matches( UNAUTHORIZED_PATTERN ) ) {
						relationship = UNAUTHORIZED_RELATIONSHIP;
						logger.debug( "Request {} is Unauthorized {}", new Object[]{ target, flowFile }  );
					}
					else {
						try {
							objectMapper.readTree( content );
							JsonNode checkpointUrl = contentNode.get( "checkpoint_url" );
							if( checkpointUrl != null ) {
								attributes.put( "checkpoint.url", checkpointUrl.asText() );
								attributes.put( "checkpoint.lock", contentNode.get( "lock" ).asText() );
								relationship = CHECKPOINT_RELATIONSHIP;
								logger.debug( "Request {} is Checkpointed {}", new Object[]{ target, flowFile } );
							}
							else {
								relationship = SUCCESS_RELATIONSHIP;
							}
						}
						catch( IOException e ) {
							relationship = UNPARSED_RELATIONSHIP;
							logger.debug( "Failed to parse request response from {} {}", new Object[]{ target, flowFile } );
						}
					}
					attributes.put( "url", targetNode.asText() );
					attributes.put( "browser", rootNode.get( "browser" ).asText() );
					attributes.put( "unixtime", rootNode.get( "unixtime" ).asText() );
					attributes.put( "request", requestNode.toString() );
					attributes.put( "request.body", requestNode.get( "body" ).toString() );
					attributes.put( "request.query", requestNode.get( "query" ).toString() );
					attributes.put( "request.cookies", requestNode.get( "cookies" ).toString() );
					attributes.put( "request.headers", requestNode.get( "headers" ).toString() );
					attributes.put( "response", responseNode.toString() );
					attributes.put( "response.cookies", responseNode.get( "cookies" ).toString() );
					attributes.put( "response.headers", responseNode.get( "headers" ).toString() );
					attributes.put( "datetime", KaNiaFi.normalizeUnixTimestamp(
						rootNode.get( "unixtime" ).asDouble(), 
						context.getProperty( DATETIME_FORMAT_PROPERTY ).getValue(),
						context.getProperty( TIMEZONE_ID_PROPERTY ).getValue()
					));
				}
				else {
					relationship = INVALID_RELATIONSHIP;
				}
				FlowFile results = session.write( flowFile, new StreamCallback() {
						@Override
						public void process( InputStream in, OutputStream out ) throws IOException {
							out.write( content.getBytes( Charset.forName( context.getProperty( CHARSET_PROPERTY ).getValue() ) ) );
						}
					}
				);
				if( context.getProperty( ALLOW_SET_ATTRIBUTE_PROPERTY ).asBoolean() ) {
					results = session.putAllAttributes( results, attributes );
					logger.info( "Successfully added Attributes {} into {}", new Object[]{
						attributes,
						results
					});
				}
				session.transfer( results, relationship );
				session.commit();
				return;
			}
			catch( Exception e ) {
				if( e instanceof IOException ) {
					logger.error( "Failed to parse FlowFile contents {}", new Object[] { flowFile } );
				}
				session.transfer( flowFile, FAILURE_RELATIONSHIP );
			}
		}
	}
	
}
