package com.fluidcode
import java.time.LocalDate


//todo : french calendar , 30 years starting from 2022
  object DateDimensionUtils {
   def generateDates(startDate: LocalDate, endDate: LocalDate): List[LocalDate] = {
     def streamDates(start: LocalDate): Stream[LocalDate] = {
       start #:: streamDates(start plusDays 1)
     }
     streamDates(startDate).takeWhile(_.isBefore(endDate.plusDays(1))).toList
   }

   def getDescription(date: LocalDate) : String = {
     date.getDayOfMonth.toString + date.getMonth + date.getYear.toString
   }

   def getQuarter(date: LocalDate) : String = {
     val resultOfEquation = (date.getMonthValue - 1)/3 + 1
     "Q" + resultOfEquation.toString
     }
   def defWeekend (date: LocalDate) : Boolean = {
     val day = date.getDayOfWeek.toString
     if (day == "SUNDAY" || day == "SATURDAY"){
       true
     }
     else {
       false
     }
   }

    def isLeapYear(date: LocalDate): Boolean = {
      if (date.getYear % 4 == 0) {
        true
      }
      else {
        false
      }
    }

   def isFirstDayOfYearAndIsIndependencyDay(date: LocalDate): Boolean = {
     val isJanuaryMonth = date.getDayOfMonth
     val isFirtDayinJan = date.getDayOfMonth
     if (isJanuaryMonth == 1 && isFirtDayinJan == 1) {
       true
     }
     else {
       false
     }
   }

   def isLabourDay(date: LocalDate) : Boolean = {
     if (date.getDayOfMonth == 1 && date.getMonthValue == 5){
       true
     }
     else {
       false
     }
   }

   def isVictoryDay(date: LocalDate) : Boolean = {
     if (date.getDayOfMonth == 8 && date.getMonthValue == 5){
       true
     }
     else {
       false
     }
   }

   def isBastilleDay(date: LocalDate) : Boolean = {
     if (date.getDayOfMonth == 14 && date.getMonthValue == 7){
       true
     }
     else {
       false
     }
   }

   def isAssumptionofMary(date: LocalDate) : Boolean = {
     if (date.getDayOfMonth == 15 && date.getMonthValue == 8){
       true
     }
     else {
       false
     }
   }

   def isAllSaintsDay(date: LocalDate) : Boolean = {
     if (date.getDayOfMonth == 1 && date.getMonthValue == 11){
       true
     }
     else {
       false
     }
   }

   def isArmisticeDay(date: LocalDate) : Boolean = {
     if (date.getDayOfMonth == 11 && date.getMonthValue == 11){
       true
     }
     else {
       false
     }
   }

   def isChristmasDay(date: LocalDate) : Boolean = {
     if (date.getDayOfMonth == 25 && date.getMonthValue == 12){
       true
     }
     else {
       false
     }
   }

   def isEasterDay(date: LocalDate): Boolean  = {
     val year = date.getYear
     val a = year % 19
     val b = year / 100
     val c = year % 100
     val d = b / 4
     val e = b % 4
     val g = (8 * b + 13) / 25
     val h = (19 * a + b - d - g + 15) % 30
     val j = c / 4
     val k = c % 4
     val m = (a + 11 * h) / 319
     val r = (2 * e + 2 * j - k - h + m + 32) % 7
     val n = (h - m + r + 90) / 25
     val p = (h - m + r + n + 19) % 32
     n match {
       case 1 => "January"
       case 2 => "February"
       case 3 => "March"
       case 4 => "April"
       case 5 => "May"
       case 6 => "June"
       case 7 => "July"
       case 8 => "August"
       case 9 => "September"
       case 10 => "October"
       case 11 => "November"
       case 12 => "December"
     }
     if (date.getDayOfMonth == p && date.getMonthValue == n) {
       true
     }
     else {
       false
     }
   }

  def isEasterMonday (date : LocalDate): Boolean = {
    val year = date.getYear
    val a = year % 19
    val b = year / 100
    val c = year % 100
    val d = b / 4
    val e = b % 4
    val g = (8 * b + 13) / 25
    val h = (19 * a + b - d - g + 15) % 30
    val j = c / 4
    val k = c % 4
    val m = (a + 11 * h) / 319
    val r = (2 * e + 2 * j - k - h + m + 32) % 7
    val n = (h - m + r + 90) / 25
    val p = (h - m + r + n + 19) % 32
    n match {
      case 1 => "January"
      case 2 => "February"
      case 3 => "March"
      case 4 => "April"
      case 5 => "May"
      case 6 => "June"
      case 7 => "July"
      case 8 => "August"
      case 9 => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
    }
    if (date.getDayOfMonth == p + 1 && date.getMonthValue == n){
      true
    }else{
      false
    }
  }

  def isAscensionDay (date : LocalDate): Boolean = {
    val year = date.getYear
    val a = year % 19
    val b = year / 100
    val c = year % 100
    val d = b / 4
    val e = b % 4
    val g = (8 * b + 13) / 25
    val h = (19 * a + b - d - g + 15) % 30
    val j = c / 4
    val k = c % 4
    val m = (a + 11 * h) / 319
    val r = (2 * e + 2 * j - k - h + m + 32) % 7
    val n = (h - m + r + 90) / 25
    val p = (h - m + r + n + 19) % 32
    n match {
      case 1 => "January"
      case 2 => "February"
      case 3 => "March"
      case 4 => "April"
      case 5 => "May"
      case 6 => "June"
      case 7 => "July"
      case 8 => "August"
      case 9 => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
    }
    if (n == 3 )
    {
      if (date.getMonthValue == 5 &&  date.getDayOfMonth == p + 39 - 61){
        return true
      }
      else{
       return false
      }
    }
    if (n == 4){
      if (date.getMonthValue == 5 && date.getDayOfMonth == p + 39 - 30)
        {
          return true
        }
      else{
        return false
      }
    }
    else
    {
      false
    }
  }

  def isWhitMonday (date : LocalDate): Boolean = {
    val year = date.getYear
    val a = year % 19
    val b = year / 100
    val c = year % 100
    val d = b / 4
    val e = b % 4
    val g = (8 * b + 13) / 25
    val h = (19 * a + b - d - g + 15) % 30
    val j = c / 4
    val k = c % 4
    val m = (a + 11 * h) / 319
    val r = (2 * e + 2 * j - k - h + m + 32) % 7
    val n = (h - m + r + 90) / 25
    val p = (h - m + r + n + 19) % 32
    n match {
      case 1 => "January"
      case 2 => "February"
      case 3 => "March"
      case 4 => "April"
      case 5 => "May"
      case 6 => "June"
      case 7 => "July"
      case 8 => "August"
      case 9 => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
    }
    if (date.getDayOfMonth == p + 49 - 60 ){
      true
    }else{
      false
    }
  }


 }