package com.fluidcode.models.silver

case class SilverDateDimensions(date: String, description : String, dayOfMonth: Int, dayOfWeek: String, month: String,
                                year: Int, quarter: Int, isWeekend: Boolean, isHoliday: Boolean)