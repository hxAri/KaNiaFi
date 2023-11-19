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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "branching", "instagram", "kanashī", "kaniafi", "request", "response" })
@CapabilityDescription( "Branching to manage Kanashī request results, Recalculate before using this processor, as it would be really resource consuming to validate each flowfile content with multiple json schemes, and also pay attention to each flowfile and FlowFile that will be passed here, this processor has more than 10 branching relationships" )
public class KaNiaFiBranching extends AbstractProcessor {

    final public static PropertyDescriptor ALLOW_SET_SCHEME_PROPERTY = new PropertyDescriptor.Builder()
        .name( "allow.set.scheme" )
        .displayName( "Allow Set Scheme Info" )
        .description( "Allow the processor set scheme.type and scheme.json attribute into FlowFile." )
        .required( false )
        .defaultValue( "true" )
        .addValidator( StandardValidators.BOOLEAN_VALIDATOR )
        .build();
    
    final public static PropertyDescriptor SCHEME_SOURCE_PROPERTY = new PropertyDescriptor.Builder()
        .name( "scheme.source" )
        .displayName( "Scheme Source" )
        .description( "Source of JSON Scheme for validate the FlowFile contents, you can download it from https://raw.githubusercontent.com/hxAri/KaNiaFi/main/nifi-kaniafi-processors/src/main/resources/schemes/scheme.json" )
        .addValidator( StandardValidators.FILE_EXISTS_VALIDATOR )
        .build();

    final public static Relationship DIRECT_RELATIONSHIP = new Relationship.Builder()
        .name( "direct" )
        .description( "FlowFile direct message inbox will be forwarded to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship EXPLORE_RELATIONSHIP = new Relationship.Builder()
        .name( "explore" )
        .description( "FlowFile Instagram explore will be directed to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship EXPLORE_CLIP_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.clip" )
        .description( "FlowFile instagram clip explore will be directed to this relation" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship EXPLORE_CLIP_MEDIA_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.clip.media" )
        .description( "FlowFile instagram clip media explore will be directed to this relation" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship EXPLORE_FILL_MEDIA_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.fill.media" )
        .description( "FlowFile instagram fill media explore will be directed to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship EXPLORE_LAYOUT_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.layout" )
        .description( "FlowFile instagram layout explore will be directed to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship EXPLORE_SECTION_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.section" )
        .description( "FlowFile instagram section explore will be directed to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
        .name( "failure" )
        .description( "When a failure occurs while parsing the contents of a FlowFile or when JSON Scheme not found or invalid" )
        .build();

    final public static Relationship FRIENDSHIP_RELATIONSHIP = new Relationship.Builder()
        .name( "friendship" )
        .description( "Friendship status will be directed to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship FRIENDSHIP_SHOW_MANY_RELATIONSHIP = new Relationship.Builder()
        .name( "friendship.show.many" )
        .description( "Multiple Friendship status will be directed to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship INBOX_RELATIONSHIP = new Relationship.Builder()
        .name( "inbox" )
        .description( "The FlowFile containing the inbox schema will be passed to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship PENDING_RELATIONSHIP = new Relationship.Builder()
        .name( "pending" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();
    
    final public static Relationship PENDINGS_RELATIONSHIP = new Relationship.Builder()
        .name( "pendings" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship PROFILE_RELATIONSHIP = new Relationship.Builder()
        .name( "profile" )
        .description( "The user's profile FlowFile will be redirected to this relationship" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_FEED_RELATIONSHIP = new Relationship.Builder()
        .name( "story.feed" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_FEED_TRAY_RELATIONSHIP = new Relationship.Builder()
        .name( "story.feed.tray" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_FEED_TRAY_REEL_RELATIONSHIP = new Relationship.Builder()
        .name( "story.feed.tray.reel" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_FEED_TRAY_REELS_RELATIONSHIP = new Relationship.Builder()
        .name( "story.feed.tray.reels" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_ITEM_RELATIONSHIP = new Relationship.Builder()
        .name( "story.item" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_HIGHLIGHT_RELATIONSHIP = new Relationship.Builder()
        .name( "story.highlight" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_HIGHLIGHTS_RELATIONSHIP = new Relationship.Builder()
        .name( "story.highlights" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_PROFILE_RELATIONSHIP = new Relationship.Builder()
        .name( "story.profile" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_PROFILE_EDGE_RELATIONSHIP = new Relationship.Builder()
        .name( "story.profile.edge" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship STORY_REEL_RELATIONSHIP = new Relationship.Builder()
        .name( "story.reel" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    final public static Relationship UNKNOWN_RELATIONSHIP = new Relationship.Builder()
        .name( "unknown" )
        .description( "The unknown scheme will be passed to this relation" )
        .build();
    
    final public static Relationship USER_RELATIONSHIP = new Relationship.Builder()
        .name( "user" )
        .description( "The FlowFile containing the user schema will be passed to this relationship" )
        .autoTerminateDefault( true )
        .build();
    
    final public static Map<KaNiaFiType, Relationship> transferable = new HashMap<>();
    static {
        transferable.put( KaNiaFiType.DIRECT, DIRECT_RELATIONSHIP );
        transferable.put( KaNiaFiType.EXPLORE, EXPLORE_RELATIONSHIP );
        transferable.put( KaNiaFiType.EXPLORE_CLIP, EXPLORE_CLIP_RELATIONSHIP );
        transferable.put( KaNiaFiType.EXPLORE_CLIP_MEDIA, EXPLORE_CLIP_MEDIA_RELATIONSHIP );
        transferable.put( KaNiaFiType.EXPLORE_FILL_MEDIA, EXPLORE_FILL_MEDIA_RELATIONSHIP );
        transferable.put( KaNiaFiType.EXPLORE_LAYOUT, EXPLORE_LAYOUT_RELATIONSHIP );
        transferable.put( KaNiaFiType.EXPLORE_SECTION, EXPLORE_SECTION_RELATIONSHIP );
        transferable.put( KaNiaFiType.FRIENDSHIP, FRIENDSHIP_RELATIONSHIP );
        transferable.put( KaNiaFiType.FRIENDSHIP_SHOW_MANY, FRIENDSHIP_SHOW_MANY_RELATIONSHIP );
        transferable.put( KaNiaFiType.INBOX, INBOX_RELATIONSHIP );
        transferable.put( KaNiaFiType.PENDING, PENDING_RELATIONSHIP );
        transferable.put( KaNiaFiType.PENDINGS, PENDINGS_RELATIONSHIP );
        transferable.put( KaNiaFiType.PROFILE, PROFILE_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_FEED, STORY_FEED_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_FEED_TRAY, STORY_FEED_TRAY_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_FEED_TRAY_REEL, STORY_FEED_TRAY_REEL_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_FEED_TRAY_REELS, STORY_FEED_TRAY_REELS_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_HIGHLIGHT, STORY_HIGHLIGHT_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_HIGHLIGHTS, STORY_HIGHLIGHTS_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_ITEM, STORY_ITEM_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_PROFILE, STORY_PROFILE_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_PROFILE_EDGE, STORY_PROFILE_EDGE_RELATIONSHIP );
        transferable.put( KaNiaFiType.STORY_REEL, STORY_REEL_RELATIONSHIP );
        transferable.put( KaNiaFiType.UNKNOWN, UNKNOWN_RELATIONSHIP );
        transferable.put( KaNiaFiType.USER, USER_RELATIONSHIP );
    }

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init( final ProcessorInitializationContext context ) {
        
        descriptors = new ArrayList<>();
        descriptors.add( ALLOW_SET_SCHEME_PROPERTY );
        descriptors.add( SCHEME_SOURCE_PROPERTY );
        descriptors = Collections.unmodifiableList( descriptors );
        
        relationships = new HashSet<>();
        relationships.add( FAILURE_RELATIONSHIP );
        for( Relationship relationship : transferable.values() ) {
            relationships.add( relationship );
        }
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
            Relationship relationship = UNKNOWN_RELATIONSHIP;
            ComponentLog logger = getLogger();
            String schemeSource = context.getProperty( SCHEME_SOURCE_PROPERTY ).getValue();
            InputStream flowFileInputStream = session.read( flowFile );
            try {
                ValidatorFactory validatorFactory = new ValidatorFactory();
                Validator validator = validatorFactory.createValidator();
                InputStream schemeInputStream = new FileInputStream( schemeSource );
                JsonNode schemeRootNode = KaNiaFi.objectMapper.readTree( schemeInputStream );
                try {
                    Map<String, String> attributes = new LinkedHashMap<>();
                    JsonNode flowFileNode = KaNiaFi.objectMapper.readTree( flowFileInputStream );
                    for( JsonNode node : schemeRootNode ) {
                        String name = node.get( "type" ).asText();
                        KaNiaFiType type = KaNiaFiType.of( name );
                        if( transferable.containsKey( type ) ) {
                            JsonNode schemeNode = node.get( "scheme" );
                            URI schemeUri = validator.registerSchema( schemeNode );
                            Result result = validator.validate( schemeUri, flowFileNode );
                            if( result.isValid() ) {
                                relationship = transferable.get( type );
                                if( type == KaNiaFiType.PROFILE ) {
                                    if( flowFileNode.has( "data" ) && 
                                        flowFileNode.get( "data" ).isObject() ) {
                                        name = "profile-graphql:variable";
                                    }
                                    else if( flowFileNode.has( "user" ) && 
                                        flowFileNode.get( "user" ).isObject() ) {
                                        name = "profile-api-info:id";
                                    }
                                    else if( flowFileNode.has( "graphql" ) && 
                                        flowFileNode.get( "graphql" ).isObject() ) {
                                        name = "profile-web-info:username";
                                    }
                                    else {
                                        type = KaNiaFiType.UNKNOWN;
                                        relationship = UNKNOWN_RELATIONSHIP;
                                    }
                                    logger.info( "Unknown Profile Scheme" );
                                }
                                logger.info( "Check value of relationship {}: {}: {}", new Object[]{ relationship, type, name } );
                                if( context.getProperty( ALLOW_SET_SCHEME_PROPERTY ).asBoolean() ) {
                                    attributes.put( "scheme.json", node.get( "scheme" ).toString() );
                                    attributes.put( "scheme.type", name );
                                }
                                flowFile = session.putAllAttributes( flowFile, attributes );
                                session.transfer( flowFile, relationship );
                                session.commit();
                                return;
                            }
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error( "Failed to parse FlowFile contents {}", new Object[] { flowFile } );
                    relationship =  FAILURE_RELATIONSHIP;
                }
            }
            catch( IOException e ) {
                if( e instanceof FileNotFoundException ) {
                    logger.error( "No such file or directory {} {}", new Object[]{ schemeSource, flowFile } );
                }
                else {
                    logger.error( "Failed to parse Scheme contents {}", new Object[] { flowFile } );
                }
                relationship = FAILURE_RELATIONSHIP;
            }
            session.transfer( flowFile, relationship );
        }
    }

}

