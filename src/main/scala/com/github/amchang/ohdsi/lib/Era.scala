package com.github.amchang.ohdsi.lib

import com.github.nscala_time.time.Imports._
import org.joda.time.Interval

/**
  * All eras builders must implement will use this as a helper serializable object
  */
object Era {

  /**
    * Build a range for a list of datetimes, 30 day interval, count included
    * @param startDateEndDateList the list of start and end date pairs
    * @return List[(DateTime, DateTime), Int] ranges, where int is amount within a range
    */
  def rangeBuilder(startDateEndDateList: List[(DateTime, DateTime)], daysOverlap: Int = 30): List[((DateTime, DateTime), Int)] = {
    var sortedStartEndDateList = startDateEndDateList.sortBy(_._1)
    // the one i'm currently looking at
    var currentRange = sortedStartEndDateList.head
    var currentAmount = 1
    // use the head as the starting point
    sortedStartEndDateList = sortedStartEndDateList diff List(currentRange)
    var finalCombine = List[((DateTime, DateTime), Int)]()

    while (sortedStartEndDateList.nonEmpty) {
      // get the next one
      val (startStartDate, startEndDate) = currentRange
      val (endStartDate, endEndDate) = sortedStartEndDateList.head
      // is it less than 30 days? keep going then
      if (new Interval(startStartDate, startEndDate.plusDays(daysOverlap)).overlaps(new Interval(endStartDate, endEndDate))) {
        currentRange = (startStartDate, if (endEndDate.isAfter(startEndDate)) endEndDate else startEndDate)
        currentAmount = currentAmount + 1
      } else {
        finalCombine = finalCombine ++ List((currentRange, currentAmount))
        currentAmount = 1
        currentRange = (endStartDate, endEndDate)
      }

      // remove the head
      sortedStartEndDateList = sortedStartEndDateList diff List(sortedStartEndDateList.head)
    }

    // fence case
    finalCombine = finalCombine ++ List((currentRange, currentAmount))

    finalCombine
  }

}
