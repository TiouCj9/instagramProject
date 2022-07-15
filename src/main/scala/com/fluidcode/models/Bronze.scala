package com.fluidcode.models




case class RawData(created_time: Long, info: Data , username: String)
case class Data (biography: String, followers_count: Long, following_count: Long, full_name: String, id: String, is_business_account: Boolean, is_joined_recently: Boolean,
                 is_private: Boolean, posts_count: Long, profile_pic_url: String)
case class GraphProfileInfoData (GraphProfileInfo: RawData)
