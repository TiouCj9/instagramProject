package com.fluidcode.models

case class DateDim(
                    date : String,
                    description : String,
                    dayOfMonth: Int,
                    dayOfWeek: String,
                    month: String,
                    year: Int,
                    quarter: String,
                    isWeekend: Boolean,
                    isHoliday: Boolean)

//Case Class for Post_Info
case class GraphImages(
                        GraphImages: Array[PostInfoData])
case class PostInfoData(
                         comments_disabled: Boolean,
                         dimensions: Dimensions,
                         display_url: String,
                         edge_media_preview_like: Edge_media_preview_like,
                         edge_media_to_caption: Edge_media_to_caption,
                         edge_media_to_comment:Edge_media_to_comment, gating_info: String, id: String, is_video: Boolean, location: String,
                         media_preview: String, owner: Owners, shortcode: String, tags: Array[String], taken_at_timestamp: Long, thumbnail_resources: Array[Thumbnail_resources],
                         thumbnail_src: String, urls: Array[String], username: String)
case class Dimensions(height: Long, width: Long)
case class Edge_media_preview_like(count: Long)
case class Edge_media_to_comment(count: Long)
case class Thumbnail_resources(config_height: Long, config_width: Long, src: String)
case class Edge_media_to_caption(edges: Array[edges])
case class edges (node: Node)
case class Node (text: String)
case class Owners(id: String)

case class GraphImagesData(__typename : String, comments : Comment)
case class GraphImagees(GraphImages: Array[GraphImagesData])
case class Comment (data: Array[Datum])
case class Datum (created_at: Long, id: String, owner: Owner, text: String)
case class Owner(id: String, profile_pic_url: String, username: String)

// case class for profileInfo
case class PostInfoS(comments_disabled: Boolean, dimensions_height: Long, dimensions_width: Long, display_url: String, edge_media_preview_like_count: Long,
                     text:String, edge_media_to_comment_count: Long, gating_info: String, id: String, is_video: Boolean,
                     location: String, media_preview: String,  owner_id: String, shortcode: String,tags: Array[String], taken_at_timestamp: Long, thumbnail_resources_config_height: Long,
                     thumbnail_resources_config_width: Long, thumbnail_resources_config_src: String, urls: Array[String], username: String)

//Case Class for Comments_Info
case class Commentse(
                      typename: String,
                      created_at: Long,
                      id: String,
                      owner_id: String,
                      owner_profile_pic_url: String,
                      owner_username: String,
                      text: String)

//Case Class for Profile_Info
case class Profile(created_time: Long, biography: String, info_followers_count: Long, info_following_count: Long, info_full_name: String,
                   info_id: String, info_is_business_account: Boolean, info_is_joined_recently: Boolean, info_is_private: Boolean, info_posts_count: Long,
                   profile_pic_url: String, username: String)
