package com.fluidcode.models.bronze

case class Data(graphImages: Array[GraphImagesElements], GraphProfileInfo: ProfileInfo)
case class GraphImagesElements(__typename: String, comments: CommentsData, comments_disabled: Boolean, dimensions: DimensionsData,
                               display_url: String, edge_media_preview_like: Likes, edge_media_to_caption: Captions,
                               edge_media_to_comment: Comments, gating_info: String, id: String, is_video: Boolean,
                               location: String, media_preview: String, owner: Owner, shortcode: String, tags: Array[String],
                               taken_at_timestamp: Long, thumbnail_resources: Array[ThumbnailElements], thumbnail_src: String,
                               urls: Array[String], username: String)
case class CommentsData(data: Array[DataElements])
case class DataElements(created_at: Long, id: String, owner: OwnerData, text: String)
case class OwnerData(id: String, profile_pic_url: String, username: String)
case class DimensionsData(height: Long, width: Long)
case class Likes(count: Long)
case class Captions(edges: Array[EdgesElements])
case class EdgesElements(node: NodeData)
case class NodeData(text: String)
case class Comments(count: Long)
case class Owner(id: String)
case class ThumbnailElements(config_height: Long, config_width: Long, src: String)
case class ProfileInfo(created_time: Long, info: Info, username: String)
case class Info(biography: String, followers_count: Long, following_count: Long, full_name: String, id: String,
                is_business_account: Boolean, is_joined_recently: Boolean, is_private: Boolean, posts_count: Long,
                profile_pic_url: String)