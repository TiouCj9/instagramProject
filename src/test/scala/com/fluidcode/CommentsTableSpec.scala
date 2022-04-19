package com.fluidcode

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommentsTableSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
 "getCommentsTable" should "extract comments data from raw data" in {

  Given("the raw data")

  When("CommentsTable Is invoked")

  Then("CommentTable should contain the same element as raw data")
 }
}
