package com.fluidcode
object Models{
  //Case Class for Comments_Info
  case class Comments(
                       typename: String,
                       created_at: Long,
                       id: String,
                       owner_id: String,
                       owner_profile_pic_url: String,
                       owner_username: String,
                       text: String)
  case class GraphImagesData(__typename : String, comments : Comment)
  case class GraphImagees(GraphImages: Array[GraphImagesData])
  case class Comment (data: Array[Datum])
  case class Datum (created_at: Long, id: String, owner: Owner, text: String)
  case class Owner(id: String, profile_pic_url: String, username: String)
  //Case Class for Profile_Info
  case class Profile(created_time: Long, biography: String, info_followers_count: Long, info_following_count: Long, info_full_name: String,
                     info_id: String, info_is_business_account: Boolean, info_is_joined_recently: Boolean, info_is_private: Boolean, info_posts_count: Long,
                     profile_pic_url: String, username: String)
  case class RawData(created_time: Long, info: Data , username: String)
  case class Data (biography: String, followers_count: Long, following_count: Long, full_name: String, id: String, is_business_account: Boolean, is_joined_recently: Boolean,
                   is_private: Boolean, posts_count: Long, profile_pic_url: String)
  case class GraphProfileInfoData (GraphProfileInfo: RawData)

  //Case Class for Post_Info
  case class GraphImages(GraphImages: Array[PostInfoData])
  case class PostInfoData(comments_disabled: Boolean, dimensions: Dimensions,
                          display_url: String, edge_media_preview_like: Edge_media_preview_like, edge_media_to_caption: Edge_media_to_caption,
                          edge_media_to_comment:Edge_media_to_comment, gating_info: String, id: String, is_video: Boolean, location: String,
                          media_preview: String, owner:Owners, shortcode: String, tags: Tags, taken_at_timestamp: Long, thumbnail_resources:Thumbnail_Resources,
                          thumbnail_src: String, urls:Urls, username: String)
  case class Dimensions(height: Long, width: Long)
  case class Edge_media_preview_like(count: Long)
  case class Edge_media_to_caption(edges: Array[edgesData])
  case class edgesData(node: node)
  case class node(text: String)
  case class Edge_media_to_comment(count: Long)
  case class Owners(id: String)
  case class Tags(tags: Array[String])
  case class Urls(urls: Array[String])
  case class Thumbnail_Resources(thumbnail_Resources: Array[Thumbnail_ResourcesData])
  case class Thumbnail_ResourcesData(config_height: Long, config_width: Long, src:String)
  case class PostInfoS(comments_disabled: Boolean, dimensions_height: Long, dimensions_width: Long, display_url: String, edge_media_preview_like_count: Long,
                       edge_media_to_caption_edges_node_text:String, edge_media_to_comment_count: Long, gating_info: String, id: String, is_video: Boolean,
                       location: String, media_preview: String,  owner_id: String, shortcode: String, col_tag1: String,col_tag2: String, taken_at_timestamp: Long, thumbnail_resources_config_height: Long,
                       thumbnail_resources_config_width: Long, thumbnail_resources_config_src: String  ,thumbnail_src: String, col_url1: String, col_url2: String, username: String)

  //case Classes for Date_Dimension
  // case class DateDim(day_of_month : DayOfMonth, day_of_week : DayOfWeek, year : Year, date_description : DateDescription, quarter : Quarter, isweekend: Boolean, isHoliday: Boolean)
case class Calendar(startDate : Int, endDate : Int)
}
