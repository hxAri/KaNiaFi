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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "branching", "instagram", "kanashī", "kaniafi", "request", "response" })
@CapabilityDescription( "Branching to manage Kanashī request results" )
public class KaNiaFiBranching extends AbstractProcessor {

    public static final PropertyDescriptor ALLOW_SET_SCHEME_PROPERTY = new PropertyDescriptor.Builder()
        .name( "allow.set.scheme" )
        .displayName( "Allow Set Scheme Info" )
        .description( "Allow the processor set scheme.type and scheme.json attribute into FlowFile." )
        .required( false )
        .defaultValue( "true" )
        .addValidator( StandardValidators.BOOLEAN_VALIDATOR )
        .build();

    public static final Relationship _RELATIONSHIP = new Relationship.Builder()
        .name( "" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();
    
    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
        .name( "failure" )
        .description( "" )
        .build();
    
    public static final Relationship DIRECT_RELATIONSHIP = new Relationship.Builder()
        .name( "direct" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship EXPLORE_RELATIONSHIP = new Relationship.Builder()
        .name( "explore" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship EXPLORE_CLIP_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.clip" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship EXPLORE_CLIP_MEDIA_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.clip.media" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship EXPLORE_FILL_MEDIA_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.fill.media" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship EXPLORE_LAYOUT_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.layout" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship EXPLORE_SECTION_RELATIONSHIP = new Relationship.Builder()
        .name( "explore.section" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship FRIENDSHIP_RELATIONSHIP = new Relationship.Builder()
        .name( "friendship" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship FRIENDSHIP_SHOW_MANY_RELATIONSHIP = new Relationship.Builder()
        .name( "friendship.show.many" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship INBOX_RELATIONSHIP = new Relationship.Builder()
        .name( "inbox" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship PENDING_RELATIONSHIP = new Relationship.Builder()
        .name( "pending" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();
    
    public static final Relationship PENDINGS_RELATIONSHIP = new Relationship.Builder()
        .name( "pendings" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship PROFILE_RELATIONSHIP = new Relationship.Builder()
        .name( "profile" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_FEED_RELATIONSHIP = new Relationship.Builder()
        .name( "story.feed" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_FEED_TRAY_RELATIONSHIP = new Relationship.Builder()
        .name( "story.feed.tray" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_FEED_TRAY_REEL_RELATIONSHIP = new Relationship.Builder()
        .name( "story.feed.tray.reel" )
        .description( "" )
        .build();

    public static final Relationship STORY_FEED_TRAY_REELS_RELATIONSHIP = new Relationship.Builder()
        .name( "story.feed.tray.reels" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_ITEM_RELATIONSHIP = new Relationship.Builder()
        .name( "story.item" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_HIGHLIGHT_RELATIONSHIP = new Relationship.Builder()
        .name( "story.highlight" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_HIGHLIGHTS_RELATIONSHIP = new Relationship.Builder()
        .name( "story.highlights" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_PROFILE_RELATIONSHIP = new Relationship.Builder()
        .name( "story.profile" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_PROFILE_EDGE_RELATIONSHIP = new Relationship.Builder()
        .name( "story.profile.edge" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship STORY_REEL_RELATIONSHIP = new Relationship.Builder()
        .name( "story.reel" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();

    public static final Relationship USER_RELATIONSHIP = new Relationship.Builder()
        .name( "user" )
        .description( "" )
        .autoTerminateDefault( true )
        .build();
    
    public static final Relationship UNKNOWN_RELATIONSHIP = new Relationship.Builder()
        .name( "unknown" )
        .description( "" )
        .build();
    
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private Map<KaNiaFiType, Relationship> tranfers = Map.of(
        KaNiaFiType.EXPLORE, EXPLORE_RELATIONSHIP,
        KaNiaFiType.EXPLORE_CLIP, EXPLORE_CLIP_RELATIONSHIP,
        KaNiaFiType.EXPLORE_CLIP_MEDIA, EXPLORE_CLIP_MEDIA_RELATIONSHIP,
        KaNiaFiType.EXPLORE_FILL_MEDIA, EXPLORE_FILL_MEDIA_RELATIONSHIP,
        KaNiaFiType.EXPLORE_LAYOUT, EXPLORE_LAYOUT_RELATIONSHIP,
        KaNiaFiType.EXPLORE_SECTION, EXPLORE_SECTION_RELATIONSHIP,
        KaNiaFiType.PROFILE, PROFILE_RELATIONSHIP,
        KaNiaFiType.UNKNOWN, UNKNOWN_RELATIONSHIP
    );

    @Override
    protected void init( final ProcessorInitializationContext context ) {
        
        descriptors = new ArrayList<>();
        descriptors.add( ALLOW_SET_SCHEME_PROPERTY );
        descriptors = Collections.unmodifiableList( descriptors );
        
        relationships = new HashSet<>();
        relationships.add( FAILURE_RELATIONSHIP );
        for( Relationship relationship : tranfers.values() ) {
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
            ComponentLog logger = getLogger();
            InputStream inputStream = session.read( flowFile );
            HashMap<String, String> attributes = new HashMap<>();
            try {
                JsonNode contentNode = KaNiaFi.objectMapper.readTree( inputStream );
                KaNiaFiType type = KaNiaFi.validateScheme( contentNode, logger );
                String scheme = type.value();
                if( type == KaNiaFiType.PROFILE ) {
                    if( contentNode.has( "data" ) && 
                        contentNode.get( "data" ).isObject() ) {
                        scheme = "profile-graphql:variable";
                    }
                    else if( contentNode.has( "user" ) && 
                        contentNode.get( "user" ).isObject() ) {
                        scheme = "profile-api-info:id";
                    }
                    else if( contentNode.has( "graphql" ) && 
                        contentNode.get( "graphql" ).isObject() ) {
                        scheme = "profile-web-info:username";
                    }
                    else {
                        type = KaNiaFiType.UNKNOWN;
                    }
                }
                if( context.getProperty( ALLOW_SET_SCHEME_PROPERTY ).asBoolean() ) {
                    attributes.put( "scheme.json", KaNiaFi.schemes.get( type ).textValue() );
                    attributes.put( "scheme.type", scheme );
                }
                flowFile = session.putAllAttributes( flowFile, attributes );
                session.transfer( flowFile, tranfers.get( type ) );
            }
            catch( IOException e ) {
                logger.error( "Failed to parse FlowFile contents {}", new Object[] { flowFile } );
                session.transfer( flowFile, FAILURE_RELATIONSHIP );
            }
            session.commit();
        }
    }

}

