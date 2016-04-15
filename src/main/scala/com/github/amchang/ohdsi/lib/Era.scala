package com.github.amchang.ohdsi.lib

import com.github.nscala_time.time.Imports._
import org.joda.time.Interval

/**
  * All eras builders must implement will use this as a helper serializable object
  */
object Era {

  /**
    * Start and end date of the range
    */
  type StartDateRange = DateTime
  type EndDateRange = DateTime

  /**
    * Amount of events in range
    */
  type RangeCount = Int

  /**
    * Gap days range
    */
  type GapDaysNoStockpile = Int
  type GapDaysStockpile = Int

  /**
    * Build a range for a list of datetimes, 30 day interval, count included
    *
    * @param startDateEndDateList the list of start and end date pairs
    * @return List[(DateTime, DateTime), Int] ranges, where int is amount within a range
    */
  def rangeBuilder(startDateEndDateList: List[(DateTime, DateTime)], daysOverlap: Int = 30):
      List[((StartDateRange, EndDateRange), RangeCount, GapDaysNoStockpile, GapDaysStockpile)] = {
    var sortedStartEndDateList = startDateEndDateList.sortBy(_._1)
    // the one i'm currently looking at
    var currentRange = sortedStartEndDateList.head
    var currentAmount = 1
    var gapDaysNoStockpile = 0
    var gapDaysStockpile = 0
    var finalCombine = List[((StartDateRange, EndDateRange), RangeCount, GapDaysNoStockpile, GapDaysStockpile)]()

    // use the head as the starting point
    sortedStartEndDateList = sortedStartEndDateList diff List(currentRange)

    while (sortedStartEndDateList.nonEmpty) {
      // get the next one
      val startRange = currentRange
      val (startStartDate, startEndDate) = currentRange
      val endRange = sortedStartEndDateList.head
      val (endStartDate, endEndDate) = endRange
      val startInterval = new Interval(startStartDate, startEndDate.plusDays(daysOverlap))
      val endInterval = new Interval(endStartDate, endEndDate)

      // is it less than daysOverlap? keep going then
      if (startInterval.overlaps(endInterval)) {
        currentRange = (startStartDate, if (endEndDate.isAfter(startEndDate)) endEndDate else startEndDate)
        currentAmount = currentAmount + 1
        gapDaysStockpile += getGapDays(startRange, endRange, true)
        gapDaysNoStockpile += getGapDays(startRange, endRange)
      } else {
        finalCombine = finalCombine ++ List((currentRange, currentAmount, gapDaysNoStockpile, gapDaysStockpile))
        // reset everything and move onto the next one
        currentAmount = 1
        gapDaysNoStockpile = 0
        gapDaysStockpile = 0
        currentRange = (endStartDate, endEndDate)
      }

      // remove the head
      sortedStartEndDateList = sortedStartEndDateList diff List(sortedStartEndDateList.head)
    }

    // fence case
    finalCombine = finalCombine ++ List((currentRange, currentAmount, gapDaysNoStockpile, gapDaysStockpile))

    finalCombine
  }

  /**
    * Get the gap days for two intervals
    * @param startRange the starting range
    * @param endRange the ending range
    * @param stockpile do we add stockpile in the interval yes or no, default is no
    * @return get the gap days for a range based on the stockpile rule
    */
  private def getGapDays(startRange: (DateTime, DateTime), endRange: (DateTime, DateTime), stockpile: Boolean = false): Int = {
    // decompose and setup intervals
    val (startStartDate, startEndDate) = startRange
    val (endStartDate, endEndDate) = endRange
    val startInterval = new Interval(startStartDate, startEndDate)
    val endInterval = new Interval(endStartDate, endEndDate)

    // see if there's a gap
    val gapInterval = startInterval.gap(endInterval)

    // if the overlap >= 0, - gap days
    if (gapInterval == null) {
      // of we stockpile, it's like - gap days
      if (stockpile) {
        - startInterval.overlap(endInterval).toDuration.getStandardDays.toInt
      } else {
        0
      }
    } else {
      //  a gap is a gap
      gapInterval.toDuration.getStandardDays.toInt
    }
  }

}
