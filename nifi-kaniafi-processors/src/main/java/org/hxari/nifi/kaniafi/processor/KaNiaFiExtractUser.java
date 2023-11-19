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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.fasterxml.jackson.databind.JsonNode;

import dev.harrel.jsonschema.Validator;
import dev.harrel.jsonschema.ValidatorFactory;
import dev.harrel.jsonschema.Validator.Result;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@Tags({ "extract", "instagram", "kanashī", "kaniafi", "request", "response", "user" })
@CapabilityDescription( "Search and extract all data containing user data, this will only extract if the data contains user schema criteria, such as username, fullname, id, or primary key" )
@SeeAlso({ KaNiaFiExtract.class })
public class KaNiaFiExtractUser extends AbstractProcessor {

	final public static PropertyDescriptor SCHEME_SOURCE_PROPERTY = new PropertyDescriptor.Builder()
        .name( "scheme.source" )
        .displayName( "Scheme Source" )
        .description( "Source of JSON Scheme for validate the FlowFile contents, you can download it from https://raw.githubusercontent.com/hxAri/KaNiaFi/main/nifi-kaniafi-processors/src/main/resources/schemes/scheme-user.json" )
        .addValidator( StandardValidators.FILE_EXISTS_VALIDATOR )
        .build();
	
	final public static PropertyDescriptor TRANSFER_TYPE_PROPERTY = new PropertyDescriptor.Builder()
        .name( "transfer.type" )
        .displayName( "Transfer Type" )
        .description( "Set the FlowFile that will be forwarded, if set to Object, each User Object will be passed one by one to the relation, if set to Array it will be made into one FlowFile with the contents of a List of User Objects" )
        .defaultValue( "Object" )
        .addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
        .build();

	final public static Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
        .name( "failure" )
        .description( "When a failure occurs while parsing the contents of a FlowFile or when JSON Scheme not found or invalid, or invalid property value" )
        .build();
	
	final public static Relationship NONE_RELATIONSHIP = new Relationship.Builder()
        .name( "none" )
        .description( "If no user is found, the Original FlowFile will be forwarded here" )
		.autoTerminateDefault( true )
        .build();
	
	final public static Relationship ORIGINAL_RELATIONSHIP = new Relationship.Builder()
        .name( "original" )
        .description( "Original flowfile will be passed to this relationship" )
		.autoTerminateDefault( true )
        .build();
	
	final public static Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
        .name( "success" )
        .description( "All user data will be passed to this relationship" )
        .build();
	
	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	@Override
	protected void init( final ProcessorInitializationContext context ) {
		
		descriptors = new ArrayList<>();
		descriptors.add( SCHEME_SOURCE_PROPERTY );
		descriptors.add( TRANSFER_TYPE_PROPERTY );
		descriptors = Collections.unmodifiableList( descriptors );
		
		relationships = new HashSet<>();
		relationships.add( FAILURE_RELATIONSHIP );
		relationships.add( NONE_RELATIONSHIP );
		relationships.add( ORIGINAL_RELATIONSHIP );
		relationships.add( SUCCESS_RELATIONSHIP );
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
            String schemeSource = context.getProperty( SCHEME_SOURCE_PROPERTY ).getValue();
            InputStream flowFileInputStream = session.read( flowFile );
            try {
                ValidatorFactory validatorFactory = new ValidatorFactory();
                Validator validator = validatorFactory.createValidator();
                InputStream schemeInputStream = new FileInputStream( schemeSource );
                JsonNode schemeRootNode = KaNiaFi.objectMapper.readTree( schemeInputStream );
				URI schemeUri = validator.registerSchema( schemeRootNode );
                try {
                    JsonNode flowFileNode = KaNiaFi.objectMapper.readTree( flowFileInputStream );
					List<JsonNode> users = extract( flowFileNode, contentNode -> validator.validate( schemeUri, contentNode ) );
					logger.info( "Found users {} on FlowFile {}", new Object[]{ users.size(), flowFile } );
					if( users.size() >= 1 ) {
						String transfer = context.getProperty( TRANSFER_TYPE_PROPERTY ).getValue();
						Relationship relationship = ORIGINAL_RELATIONSHIP;
						if( transfer.equalsIgnoreCase( "Object" ) ) {
							List<FlowFile> flowFiles = new ArrayList<>();
							for( JsonNode user : users ) {
								String userJson = user.toString();
								FlowFile userFlowFile = session.create();
								userFlowFile = session.putAllAttributes( userFlowFile, copy( flowFile, userFlowFile ) );
								userFlowFile = session.write( userFlowFile, outputStream -> {
									outputStream.write( userJson.getBytes( Charset.forName( "UTF-8" ) ) );
								});
								flowFiles.add( userFlowFile );
							}
							session.transfer( flowFiles, SUCCESS_RELATIONSHIP );
						}
						else if( transfer.equalsIgnoreCase( "Array" ) ) {
							String usersJson = users.toString();
							FlowFile usersFlowFile = session.create();
							usersFlowFile = session.putAllAttributes( usersFlowFile, copy( flowFile, usersFlowFile ) );
							usersFlowFile = session.write( usersFlowFile, outputStream -> {
								outputStream.write( usersJson.getBytes( Charset.forName( "UTF-8" ) ) );
							});
							session.transfer( usersFlowFile, SUCCESS_RELATIONSHIP );
						}
						else {
							relationship = FAILURE_RELATIONSHIP;
							logger.error( "Invalid FlowFile transfer type {}", new Object[]{ flowFile } );
						}
						session.transfer( flowFile, relationship );
					}
					else {
						session.transfer( flowFile, NONE_RELATIONSHIP );
					}
                }
                catch( IOException e ) {
                    logger.error( "Failed to parse FlowFile contents {}", new Object[] { flowFile } );
					session.transfer( flowFile, FAILURE_RELATIONSHIP );
                }
            }
            catch( IOException e ) {
                if( e instanceof FileNotFoundException ) {
                    logger.error( "No such file or directory {} {}", new Object[]{ schemeSource, flowFile } );
                }
                else {
                    logger.error( "Failed to parse Scheme contents {}", new Object[] { flowFile } );
                }
				session.transfer( flowFile, FAILURE_RELATIONSHIP );
            }
		}
	}

	private Map<String, String> copy( FlowFile source , FlowFile flowFile ) {
		Map<String, String> attributes = new HashMap<>();
		Map<String, String> parent = source.getAttributes();
		for( String key : parent.keySet() ) {
			String value = parent.get( key );
			if( flowFile.getAttribute( key ) == null ) {
				attributes.put( key, value );
			}
		}
		attributes.put( "scheme.type", "user" );
		return attributes;
	}

	private List<JsonNode> extract( JsonNode rootNode, Function<JsonNode, Result> validator ) {
		List<JsonNode> results = new ArrayList<>();
		if( validator.apply( rootNode ).isValid() ) {
			results.add( rootNode );
		}
        for( JsonNode childNode : rootNode ) {
            if( childNode.isArray() || childNode.isObject() ) {
                for( JsonNode item : childNode ) {
                    results.addAll( extract( item, validator ) );
                }
            }
        }
		return results;
	}
	
}

