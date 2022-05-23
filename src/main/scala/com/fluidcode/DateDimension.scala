package com.fluidcode
import java.time.LocalDate

 // (23, Saturday, 2022, "january 1, 2022" , Q3, true, true)
//todo : french calendar , 30 years starting from 2022
  object DateDimensionUtils {
   def generateDates(startDate: LocalDate, endDate: LocalDate): List[LocalDate] = {
     def streamDates(start: LocalDate): Stream[LocalDate] = {
       start #:: streamDates(start plusDays (1))
     }
     streamDates(startDate).takeWhile(_.isBefore(endDate.plusDays(1))).toList
   }

   def getDescription(date: LocalDate) : String = {
     date.getDayOfMonth.toString + date.getMonth + date.getYear.toString
   }

   def getQuarter(date: LocalDate) : String = {
     date.getMonthValue match {
       case 1 => "Q1"
       case 2 => "Q1"
       case 3 => "Q1"
       case 4 => "Q2"
       case 5 => "Q2"
       case 6 => "Q2"
       case 7 => "Q3"
       case 8 => "Q3"
       case 9 => "Q3"
       case 10 => "Q4"
       case 11 => "Q4"
       case 12 => "Q4"
         //todo : find math equation "Q" + resultOfEquation.toString
     }
   }


 }

