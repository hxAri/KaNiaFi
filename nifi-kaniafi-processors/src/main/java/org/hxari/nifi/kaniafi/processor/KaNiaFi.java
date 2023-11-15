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

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.logging.ComponentLog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.harrel.jsonschema.Validator;
import dev.harrel.jsonschema.Validator.Result;
import dev.harrel.jsonschema.ValidatorFactory;

public class KaNiaFi {

    final public static String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    final public static String DATETIME_TIMEZONE = "Asia/Tokyo";
    
    final public static ObjectMapper objectMapper = new ObjectMapper();
    final public static Map<KaNiaFiType, JsonNode> schemes = new HashMap<>();
    static {
        Map<KaNiaFiType, String> map = Map.of( 
            KaNiaFiType.EXPLORE, "{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"auto_load_more_enabled\":{\"type\":\"boolean\"},\"clusters\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"max_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"more_available\":{\"type\":\"boolean\"},\"next_max_id\":{\"type\":\"string\"},\"rank_token\":{\"type\":\"string\"},\"ranked_time_in_seconds\":{\"type\":\"integer\"},\"sectional_items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"explore_item_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"feed_type\":{\"type\":\"string\"},\"layout_content\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"one_by_two_item\":{\"type\":\"object\",\"properties\":{\"clips\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"content_source\":{\"type\":\"string\"},\"design\":{\"type\":\"string\"},\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"media\"]}},\"label\":{\"type\":\"string\"},\"max_id\":{\"type\":\"string\"},\"more_available\":{\"type\":\"boolean\"},\"type\":{\"type\":\"string\"}},\"required\":[\"id\",\"items\",\"max_id\"]}},\"required\":[\"clips\"]},\"fill_items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"media\"]}}},\"required\":[\"fill_items\",\"one_by_two_item\"]},\"layout_type\":{\"type\":\"string\"}},\"required\":[\"layout_content\"]}},\"session_paging_token\":{\"type\":\"string\"}}}",
            KaNiaFiType.EXPLORE_CLIP, "{\"anyOf\":[{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"explore_item_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"feed_type\":{\"type\":\"string\"},\"layout_content\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"one_by_two_item\":{\"type\":\"object\",\"properties\":{\"clips\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"content_source\":{\"type\":\"string\"},\"design\":{\"type\":\"string\"},\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"media\"]}},\"label\":{\"type\":\"string\"},\"max_id\":{\"type\":\"string\"},\"more_available\":{\"type\":\"boolean\"},\"type\":{\"type\":\"string\"}},\"required\":[\"id\",\"items\",\"max_id\"]}},\"required\":[\"clips\"]},\"fill_items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"media\"]}}},\"required\":[\"fill_items\",\"one_by_two_item\"]},\"layout_type\":{\"type\":\"string\"}},\"required\":[\"layout_content\"]}},{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"explore_item_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"feed_type\":{\"type\":\"string\"},\"layout_content\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"one_by_two_item\":{\"type\":\"object\",\"properties\":{\"clips\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"content_source\":{\"type\":\"string\"},\"design\":{\"type\":\"string\"},\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"media\"]}},\"label\":{\"type\":\"string\"},\"max_id\":{\"type\":\"string\"},\"more_available\":{\"type\":\"boolean\"},\"type\":{\"type\":\"string\"}},\"required\":[\"id\",\"items\",\"max_id\"]}},\"required\":[\"clips\"]},\"fill_items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"media\"]}}},\"required\":[\"fill_items\",\"one_by_two_item\"]},\"layout_type\":{\"type\":\"string\"}},\"required\":[\"layout_content\"]}]}",
            KaNiaFiType.EXPLORE_CLIP_MEDIA, "{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"one_by_two_item\":{\"type\":\"object\",\"properties\":{\"clips\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"content_source\":{\"type\":\"string\"},\"design\":{\"type\":\"string\"},\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"media\"]}},\"label\":{\"type\":\"string\"},\"max_id\":{\"type\":\"string\"},\"more_available\":{\"type\":\"boolean\"},\"type\":{\"type\":\"string\"}},\"required\":[\"id\",\"items\",\"max_id\"]}},\"required\":[\"clips\"]},\"fill_items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"media\"]}}},\"required\":[\"fill_items\",\"one_by_two_item\"]}",
            KaNiaFiType.EXPLORE_FILL_MEDIA, "{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"media\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"can_see_insights_as_brand\":{\"type\":\"boolean\"},\"can_view_more_preview_comments\":{\"type\":\"boolean\"},\"can_viewer_reshare\":{\"type\":\"boolean\"},\"can_viewer_save\":{\"type\":\"boolean\"},\"caption\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"media_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}}},\"caption_is_edited\":{\"type\":\"boolean\"},\"client_cache_key\":{\"type\":\"string\"},\"clips_delivery_parameters\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"clips_metadata\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"code\":{\"type\":\"string\"},\"comment_count\":{\"type\":\"number\"},\"comment_inform_treatment\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"comment_likes_enabled\":{\"type\":\"boolean\"},\"comment_threading_enabled\":{\"type\":\"boolean\"},\"comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"text\":{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"user_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}}}},\"commerciality_status\":{\"type\":\"string\"},\"deleted_reason\":{\"type\":\"integer\"},\"device_timestamp\":{\"type\":\"number\"},\"enable_media_notes_production\":{\"type\":\"boolean\"},\"enable_waist\":{\"type\":\"boolean\"},\"explore_hide_comments\":{\"type\":\"boolean\"},\"facepile_top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"fb_user_tags\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"filter_type\":{\"type\":\"integer\"},\"has_audio\":{\"type\":\"boolean\"},\"has_delayed_metadata\":{\"type\":\"boolean\"},\"has_liked\":{\"type\":\"boolean\"},\"has_more_comments\":{\"type\":\"boolean\"},\"has_shared_to_fb\":{\"type\":\"boolean\"},\"hide_view_all_comment_entrypoint\":{\"type\":\"boolean\"},\"id\":{\"type\":\"string\"},\"ig_media_sharing_disabled\":{\"type\":\"boolean\"},\"image_versions2\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"integrity_review_decision\":{\"type\":\"string\"},\"inventory_source\":{\"type\":\"string\"},\"is_artist_pick\":{\"type\":\"boolean\"},\"is_auto_created\":{\"type\":\"boolean\"},\"is_comments_gif_composer_enabled\":{\"type\":\"boolean\"},\"is_cutout_sticker_allowed\":{\"type\":\"boolean\"},\"is_dash_eligible\":{\"type\":\"boolean\"},\"is_in_profile_grid\":{\"type\":\"boolean\"},\"is_open_to_public_submission\":{\"type\":\"boolean\"},\"is_organic_product_tagging_eligible\":{\"type\":\"boolean\"},\"is_paid_partnership\":{\"type\":\"boolean\"},\"is_post_live_clips_media\":{\"type\":\"boolean\"},\"is_reshare_of_text_post_app_media_in_ig\":{\"type\":\"boolean\"},\"is_third_party_downloads_eligible\":{\"type\":\"boolean\"},\"is_unified_video\":{\"type\":\"boolean\"},\"is_visual_reply_commenter_notice_enabled\":{\"type\":\"boolean\"},\"like_and_view_counts_disabled\":{\"type\":\"boolean\"},\"like_count\":{\"type\":\"number\"},\"logging_info_token\":{\"type\":\"string\"},\"max_num_visible_preview_comments\":{\"type\":\"integer\"},\"media_appreciation_settings\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_cropping_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_notes\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_type\":{\"type\":\"integer\"},\"mezql_token\":{\"type\":\"string\"},\"music_metadata\":{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}]},\"next_max_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"number_of_qualities\":{\"type\":\"integer\"},\"organic_tracking_token\":{\"type\":\"string\"},\"original_height\":{\"type\":\"integer\"},\"original_media_has_visual_reply_media\":{\"type\":\"boolean\"},\"original_width\":{\"type\":\"integer\"},\"owner\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"play_count\":{\"type\":\"number\"},\"preview_comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"product_type\":{\"type\":\"string\"},\"profile_grid_control_enabled\":{\"type\":\"boolean\"},\"recommendation_data\":{\"type\":\"string\"},\"sharing_friction_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"should_request_ads\":{\"type\":\"boolean\"},\"strong_id__\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"taken_at\":{\"type\":\"integer\"},\"top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"required\":[\"code\",\"id\",\"pk\",\"user\"]}},\"required\":[\"media\"]}}},\"required\":[\"id\",\"items\"]}",
            KaNiaFiType.EXPLORE_LAYOUT, "{\"anyOf\":[{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"can_see_insights_as_brand\":{\"type\":\"boolean\"},\"can_view_more_preview_comments\":{\"type\":\"boolean\"},\"can_viewer_reshare\":{\"type\":\"boolean\"},\"can_viewer_save\":{\"type\":\"boolean\"},\"caption\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"media_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}}},\"caption_is_edited\":{\"type\":\"boolean\"},\"client_cache_key\":{\"type\":\"string\"},\"clips_delivery_parameters\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"clips_metadata\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"code\":{\"type\":\"string\"},\"comment_count\":{\"type\":\"number\"},\"comment_inform_treatment\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"comment_likes_enabled\":{\"type\":\"boolean\"},\"comment_threading_enabled\":{\"type\":\"boolean\"},\"comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"text\":{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"user_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}}}},\"commerciality_status\":{\"type\":\"string\"},\"deleted_reason\":{\"type\":\"integer\"},\"device_timestamp\":{\"type\":\"number\"},\"enable_media_notes_production\":{\"type\":\"boolean\"},\"enable_waist\":{\"type\":\"boolean\"},\"explore_hide_comments\":{\"type\":\"boolean\"},\"facepile_top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"fb_user_tags\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"filter_type\":{\"type\":\"integer\"},\"has_audio\":{\"type\":\"boolean\"},\"has_delayed_metadata\":{\"type\":\"boolean\"},\"has_liked\":{\"type\":\"boolean\"},\"has_more_comments\":{\"type\":\"boolean\"},\"has_shared_to_fb\":{\"type\":\"boolean\"},\"hide_view_all_comment_entrypoint\":{\"type\":\"boolean\"},\"id\":{\"type\":\"string\"},\"ig_media_sharing_disabled\":{\"type\":\"boolean\"},\"image_versions2\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"integrity_review_decision\":{\"type\":\"string\"},\"inventory_source\":{\"type\":\"string\"},\"is_artist_pick\":{\"type\":\"boolean\"},\"is_auto_created\":{\"type\":\"boolean\"},\"is_comments_gif_composer_enabled\":{\"type\":\"boolean\"},\"is_cutout_sticker_allowed\":{\"type\":\"boolean\"},\"is_dash_eligible\":{\"type\":\"boolean\"},\"is_in_profile_grid\":{\"type\":\"boolean\"},\"is_open_to_public_submission\":{\"type\":\"boolean\"},\"is_organic_product_tagging_eligible\":{\"type\":\"boolean\"},\"is_paid_partnership\":{\"type\":\"boolean\"},\"is_post_live_clips_media\":{\"type\":\"boolean\"},\"is_reshare_of_text_post_app_media_in_ig\":{\"type\":\"boolean\"},\"is_third_party_downloads_eligible\":{\"type\":\"boolean\"},\"is_unified_video\":{\"type\":\"boolean\"},\"is_visual_reply_commenter_notice_enabled\":{\"type\":\"boolean\"},\"like_and_view_counts_disabled\":{\"type\":\"boolean\"},\"like_count\":{\"type\":\"number\"},\"logging_info_token\":{\"type\":\"string\"},\"max_num_visible_preview_comments\":{\"type\":\"integer\"},\"media_appreciation_settings\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_cropping_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_notes\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_type\":{\"type\":\"integer\"},\"mezql_token\":{\"type\":\"string\"},\"music_metadata\":{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}]},\"next_max_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"number_of_qualities\":{\"type\":\"integer\"},\"organic_tracking_token\":{\"type\":\"string\"},\"original_height\":{\"type\":\"integer\"},\"original_media_has_visual_reply_media\":{\"type\":\"boolean\"},\"original_width\":{\"type\":\"integer\"},\"owner\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"play_count\":{\"type\":\"number\"},\"preview_comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"product_type\":{\"type\":\"string\"},\"profile_grid_control_enabled\":{\"type\":\"boolean\"},\"recommendation_data\":{\"type\":\"string\"},\"sharing_friction_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"should_request_ads\":{\"type\":\"boolean\"},\"strong_id__\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"taken_at\":{\"type\":\"integer\"},\"top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}},\"required\":[\"code\",\"id\",\"pk\",\"user\"]}},{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"can_see_insights_as_brand\":{\"type\":\"boolean\"},\"can_view_more_preview_comments\":{\"type\":\"boolean\"},\"can_viewer_reshare\":{\"type\":\"boolean\"},\"can_viewer_save\":{\"type\":\"boolean\"},\"caption\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"media_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}}},\"caption_is_edited\":{\"type\":\"boolean\"},\"client_cache_key\":{\"type\":\"string\"},\"clips_delivery_parameters\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"clips_metadata\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"code\":{\"type\":\"string\"},\"comment_count\":{\"type\":\"number\"},\"comment_inform_treatment\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"comment_likes_enabled\":{\"type\":\"boolean\"},\"comment_threading_enabled\":{\"type\":\"boolean\"},\"comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"text\":{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"user_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}}}},\"commerciality_status\":{\"type\":\"string\"},\"deleted_reason\":{\"type\":\"integer\"},\"device_timestamp\":{\"type\":\"number\"},\"enable_media_notes_production\":{\"type\":\"boolean\"},\"enable_waist\":{\"type\":\"boolean\"},\"explore_hide_comments\":{\"type\":\"boolean\"},\"facepile_top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"fb_user_tags\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"filter_type\":{\"type\":\"integer\"},\"has_audio\":{\"type\":\"boolean\"},\"has_delayed_metadata\":{\"type\":\"boolean\"},\"has_liked\":{\"type\":\"boolean\"},\"has_more_comments\":{\"type\":\"boolean\"},\"has_shared_to_fb\":{\"type\":\"boolean\"},\"hide_view_all_comment_entrypoint\":{\"type\":\"boolean\"},\"id\":{\"type\":\"string\"},\"ig_media_sharing_disabled\":{\"type\":\"boolean\"},\"image_versions2\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"integrity_review_decision\":{\"type\":\"string\"},\"inventory_source\":{\"type\":\"string\"},\"is_artist_pick\":{\"type\":\"boolean\"},\"is_auto_created\":{\"type\":\"boolean\"},\"is_comments_gif_composer_enabled\":{\"type\":\"boolean\"},\"is_cutout_sticker_allowed\":{\"type\":\"boolean\"},\"is_dash_eligible\":{\"type\":\"boolean\"},\"is_in_profile_grid\":{\"type\":\"boolean\"},\"is_open_to_public_submission\":{\"type\":\"boolean\"},\"is_organic_product_tagging_eligible\":{\"type\":\"boolean\"},\"is_paid_partnership\":{\"type\":\"boolean\"},\"is_post_live_clips_media\":{\"type\":\"boolean\"},\"is_reshare_of_text_post_app_media_in_ig\":{\"type\":\"boolean\"},\"is_third_party_downloads_eligible\":{\"type\":\"boolean\"},\"is_unified_video\":{\"type\":\"boolean\"},\"is_visual_reply_commenter_notice_enabled\":{\"type\":\"boolean\"},\"like_and_view_counts_disabled\":{\"type\":\"boolean\"},\"like_count\":{\"type\":\"number\"},\"logging_info_token\":{\"type\":\"string\"},\"max_num_visible_preview_comments\":{\"type\":\"integer\"},\"media_appreciation_settings\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_cropping_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_notes\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_type\":{\"type\":\"integer\"},\"mezql_token\":{\"type\":\"string\"},\"music_metadata\":{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}]},\"next_max_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"number_of_qualities\":{\"type\":\"integer\"},\"organic_tracking_token\":{\"type\":\"string\"},\"original_height\":{\"type\":\"integer\"},\"original_media_has_visual_reply_media\":{\"type\":\"boolean\"},\"original_width\":{\"type\":\"integer\"},\"owner\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"play_count\":{\"type\":\"number\"},\"preview_comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"product_type\":{\"type\":\"string\"},\"profile_grid_control_enabled\":{\"type\":\"boolean\"},\"recommendation_data\":{\"type\":\"string\"},\"sharing_friction_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"should_request_ads\":{\"type\":\"boolean\"},\"strong_id__\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"taken_at\":{\"type\":\"integer\"},\"top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}},\"required\":[\"code\",\"id\",\"pk\",\"user\"]}]}",
            KaNiaFiType.EXPLORE_SECTION, "{\"anyOf\":[{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"algorithm\":{\"type\":\"string\"},\"can_see_insights_as_brand\":{\"type\":\"boolean\"},\"can_view_more_preview_comments\":{\"type\":\"boolean\"},\"can_viewer_reshare\":{\"type\":\"boolean\"},\"can_viewer_save\":{\"type\":\"boolean\"},\"caption\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"media_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}}},\"caption_is_edited\":{\"type\":\"boolean\"},\"carousel_media\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"carousel_media_count\":{\"type\":\"integer\"},\"carousel_media_ids\":{\"type\":\"array\",\"items\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}},\"carousel_media_pending_post_count\":{\"type\":\"integer\"},\"client_cache_key\":{\"type\":\"string\"},\"coauthor_producers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}},\"code\":{\"type\":\"string\"},\"comment_count\":{\"type\":\"integer\"},\"comment_inform_treatment\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"comment_threading_enabled\":{\"type\":\"boolean\"},\"comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"text\":{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"user_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}}}},\"commerciality_status\":{\"type\":\"string\"},\"deleted_reason\":{\"type\":\"integer\"},\"device_timestamp\":{\"type\":\"number\"},\"enable_media_notes_production\":{\"type\":\"boolean\"},\"enable_waist\":{\"type\":\"boolean\"},\"explore\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"explore_context\":{\"type\":\"string\"},\"explore_hide_comments\":{\"type\":\"boolean\"},\"facepile_top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"fb_user_tags\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"filter_type\":{\"type\":\"integer\"},\"has_delayed_metadata\":{\"type\":\"boolean\"},\"has_liked\":{\"type\":\"boolean\"},\"has_more_comments\":{\"type\":\"boolean\"},\"has_shared_to_fb\":{\"type\":\"boolean\"},\"id\":{\"type\":\"string\"},\"image_versions2\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"impression_token\":{\"type\":\"string\"},\"integrity_review_decision\":{\"type\":\"string\"},\"inventory_source\":{\"type\":\"string\"},\"is_auto_created\":{\"type\":\"boolean\"},\"is_comments_gif_composer_enabled\":{\"type\":\"boolean\"},\"is_cutout_sticker_allowed\":{\"type\":\"boolean\"},\"is_in_profile_grid\":{\"type\":\"boolean\"},\"is_open_to_public_submission\":{\"type\":\"boolean\"},\"is_organic_product_tagging_eligible\":{\"type\":\"boolean\"},\"is_paid_partnership\":{\"type\":\"boolean\"},\"is_post_live_clips_media\":{\"type\":\"boolean\"},\"is_reshare_of_text_post_app_media_in_ig\":{\"type\":\"boolean\"},\"is_unified_video\":{\"type\":\"boolean\"},\"is_visual_reply_commenter_notice_enabled\":{\"type\":\"boolean\"},\"like_and_view_counts_disabled\":{\"type\":\"boolean\"},\"like_count\":{\"type\":\"integer\"},\"logging_info_token\":{\"type\":\"string\"},\"max_num_visible_preview_comments\":{\"type\":\"integer\"},\"media_notes\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_type\":{\"type\":\"integer\"},\"mezql_token\":{\"type\":\"string\"},\"music_metadata\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"organic_tracking_token\":{\"type\":\"string\"},\"original_height\":{\"type\":\"integer\"},\"original_media_has_visual_reply_media\":{\"type\":\"boolean\"},\"original_width\":{\"type\":\"integer\"},\"photo_of_you\":{\"type\":\"boolean\"},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"preview_comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"product_type\":{\"type\":\"string\"},\"profile_grid_control_enabled\":{\"type\":\"boolean\"},\"recommendation_data\":{\"type\":\"string\"},\"sharing_friction_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"should_request_ads\":{\"type\":\"boolean\"},\"strong_id__\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"taken_at\":{\"type\":\"integer\"},\"top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}},\"usertags\":{\"type\":\"string\"}},\"required\":[\"algorithm\",\"code\",\"id\",\"pk\",\"user\"]}},{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"algorithm\":{\"type\":\"string\"},\"can_see_insights_as_brand\":{\"type\":\"boolean\"},\"can_view_more_preview_comments\":{\"type\":\"boolean\"},\"can_viewer_reshare\":{\"type\":\"boolean\"},\"can_viewer_save\":{\"type\":\"boolean\"},\"caption\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"media_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}}},\"caption_is_edited\":{\"type\":\"boolean\"},\"carousel_media\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"carousel_media_count\":{\"type\":\"integer\"},\"carousel_media_ids\":{\"type\":\"array\",\"items\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}},\"carousel_media_pending_post_count\":{\"type\":\"integer\"},\"client_cache_key\":{\"type\":\"string\"},\"coauthor_producers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}},\"code\":{\"type\":\"string\"},\"comment_count\":{\"type\":\"integer\"},\"comment_inform_treatment\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"comment_threading_enabled\":{\"type\":\"boolean\"},\"comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"created_at\":{\"type\":\"integer\"},\"created_at_utc\":{\"type\":\"number\"},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"text\":{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]},\"user_id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}}}}},\"commerciality_status\":{\"type\":\"string\"},\"deleted_reason\":{\"type\":\"integer\"},\"device_timestamp\":{\"type\":\"number\"},\"enable_media_notes_production\":{\"type\":\"boolean\"},\"enable_waist\":{\"type\":\"boolean\"},\"explore\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"explore_context\":{\"type\":\"string\"},\"explore_hide_comments\":{\"type\":\"boolean\"},\"facepile_top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"fb_user_tags\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"filter_type\":{\"type\":\"integer\"},\"has_delayed_metadata\":{\"type\":\"boolean\"},\"has_liked\":{\"type\":\"boolean\"},\"has_more_comments\":{\"type\":\"boolean\"},\"has_shared_to_fb\":{\"type\":\"boolean\"},\"id\":{\"type\":\"string\"},\"image_versions2\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"impression_token\":{\"type\":\"string\"},\"integrity_review_decision\":{\"type\":\"string\"},\"inventory_source\":{\"type\":\"string\"},\"is_auto_created\":{\"type\":\"boolean\"},\"is_comments_gif_composer_enabled\":{\"type\":\"boolean\"},\"is_cutout_sticker_allowed\":{\"type\":\"boolean\"},\"is_in_profile_grid\":{\"type\":\"boolean\"},\"is_open_to_public_submission\":{\"type\":\"boolean\"},\"is_organic_product_tagging_eligible\":{\"type\":\"boolean\"},\"is_paid_partnership\":{\"type\":\"boolean\"},\"is_post_live_clips_media\":{\"type\":\"boolean\"},\"is_reshare_of_text_post_app_media_in_ig\":{\"type\":\"boolean\"},\"is_unified_video\":{\"type\":\"boolean\"},\"is_visual_reply_commenter_notice_enabled\":{\"type\":\"boolean\"},\"like_and_view_counts_disabled\":{\"type\":\"boolean\"},\"like_count\":{\"type\":\"integer\"},\"logging_info_token\":{\"type\":\"string\"},\"max_num_visible_preview_comments\":{\"type\":\"integer\"},\"media_notes\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"media_type\":{\"type\":\"integer\"},\"mezql_token\":{\"type\":\"string\"},\"music_metadata\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"organic_tracking_token\":{\"type\":\"string\"},\"original_height\":{\"type\":\"integer\"},\"original_media_has_visual_reply_media\":{\"type\":\"boolean\"},\"original_width\":{\"type\":\"integer\"},\"photo_of_you\":{\"type\":\"boolean\"},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"preview_comments\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"product_type\":{\"type\":\"string\"},\"profile_grid_control_enabled\":{\"type\":\"boolean\"},\"recommendation_data\":{\"type\":\"string\"},\"sharing_friction_info\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}},\"should_request_ads\":{\"type\":\"boolean\"},\"strong_id__\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"taken_at\":{\"type\":\"integer\"},\"top_likers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{}}},\"user\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}}},\"usertags\":{\"type\":\"string\"}},\"required\":[\"algorithm\",\"code\",\"id\",\"pk\",\"user\"]}]}",
            KaNiaFiType.PROFILE, "{\"type\":\"object\",\"anyOf\":[{\"properties\":{\"data\":{\"type\":\"object\",\"properties\":{\"user\":{\"type\":\"object\",\"properties\":{\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}},\"required\":[\"username\"]}},\"required\":[\"user\"]}},\"required\":[\"data\"]},{\"properties\":{\"graphql\":{\"type\":\"object\",\"properties\":{\"user\":{\"type\":\"object\",\"properties\":{\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}},\"required\":[\"username\"]}},\"required\":[\"user\"]}},\"required\":[\"graphql\"]},{\"properties\":{\"user\":{\"type\":\"object\",\"properties\":{\"id\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"pk\":{\"anyOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]},\"username\":{\"type\":\"string\"}},\"required\":[\"username\"]}},\"required\":[\"user\"]}]}"
        );
        try {
            for( KaNiaFiType key : map.keySet() ) {
                schemes.put( key, objectMapper.readTree( map.get( key ) ) );
            }
        }
        catch( IOException e ) {
            e.printStackTrace();
        }
    }

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

    public static KaNiaFiType validateScheme( JsonNode contentNode, ComponentLog logger ) throws IOException {
        ValidatorFactory validatorFactory = new ValidatorFactory();
        Validator validator = validatorFactory.createValidator();
        for( KaNiaFiType type : schemes.keySet() ) {
            try {
                URI uri = validator.registerSchema( schemes.get( type ) );
                Result result = validator.validate( uri, contentNode );
                if( result.isValid() ) {
                    return type;
                }
            }
            catch( IllegalArgumentException e ) {
                logger.error( "{}: {}: {}", new Object[]{ e.getClass().getSimpleName(), type.name(), e.getMessage() } );
            }
        }
        return KaNiaFiType.UNKNOWN;
    }

}
