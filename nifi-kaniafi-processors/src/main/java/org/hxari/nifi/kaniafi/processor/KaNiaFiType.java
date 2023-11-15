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

public enum KaNiaFiType {

	DIRECT( "direct" ),
	EXPLORE( "explore:grid" ),
	EXPLORE_CLIP( "explore:clip" ),
	EXPLORE_CLIP_MEDIA( "explore:clip-media" ),
	// EXPLORE_CLIP_ITEM( "ExploreClipItem" ),
	// EXPLORE_FILL_ITEM( "ExploreFillItem" ),
	EXPLORE_FILL_MEDIA( "explore:fill-media" ),
	EXPLORE_LAYOUT( "explore:layout" ),
	EXPLORE_SECTION( "explore:section" ),
	FRIENDSHIP( "friendship:single" ),
	FRIENDSHIP_SHOW_MANY( "friendship:many" ),
	INBOX( "inbox" ),
	PENDING( "pending:single" ),
	PENDINGS( "pending:container" ),
	PROFILE( "profile" ),
	STORY_FEED( "story:feed" ),
	STORY_FEED_TRAY( "story:feed-tray" ),
	STORY_FEED_TRAY_REEL( "story:feed-tray-reel" ),
	STORY_FEED_TRAY_REELS( "story:feed-tray-reel-container" ),
	STORY_ITEM( "story:item" ),
	STORY_HIGHLIGHT( "story:highlight" ),
	STORY_HIGHLIGHTS( "story:highlight:container" ),
	STORY_PROFILE( "story:profile" ),
	STORY_PROFILE_EDGE( "story:profile-edge" ),
	STORY_REEL( "story:reel" ),
	UNKNOWN( "unknown" ),
	USER( "user" );

	private String type;

	private KaNiaFiType( String type ) {
		this.type = type;
	}

	@Override
	public String toString() {
		return String.format( "\u003c\u0025\u0073\u0020\u0025\u0073\u002f\u003e", this.name(), this.type );
	}
	
	public String value() {
		return this.type;
	}
}
