package com.github.amchang.ohdsi.lib

import org.joda.time.DateTime
import org.scalatest._

/**
  * Test out the era singleton
  */
class EraSpec extends FunSpec {

  describe("Era") {

    describe("rangeBuilder") {

      it("returns an empty list when the input list") {
        val emptyList = Era.rangeBuilder(List())
        assert(emptyList.isEmpty)
      }

      it("returns an empty list when the daysOverlap parameter is less than 0") {
        val emptyList = Era.rangeBuilder(List((DateTime.now.withTimeAtStartOfDay, DateTime.now.withTimeAtStartOfDay)), -1)
        assert(emptyList.isEmpty)
      }

      it("returns a date range for a single date range set with same start and end date with overlap of 30 days") {
        val metaList = Era.rangeBuilder(List((DateTime.now.withTimeAtStartOfDay, DateTime.now.withTimeAtStartOfDay)))
        val ((startDateRange, endDateRange), rangeCount, gapDaysNoStockpile, gapDaysStockpile) = metaList.head

        assert(startDateRange.withTimeAtStartOfDay.equals(DateTime.now.withTimeAtStartOfDay))
        assert(endDateRange.withTimeAtStartOfDay.equals(DateTime.now.withTimeAtStartOfDay))
        assert(rangeCount == 1)
        assert(gapDaysNoStockpile == 0)
        assert(gapDaysStockpile == 0)
      }

      it("returns a date range for a single date range set with different start and end date with overlap of 30 days") {
        val endDate = DateTime.now.plusDays(15)
        val metaList = Era.rangeBuilder(List((DateTime.now.withTimeAtStartOfDay, endDate)))
        val ((startDateRange, endDateRange), rangeCount, gapDaysNoStockpile, gapDaysStockpile) = metaList.head

        assert(startDateRange.withTimeAtStartOfDay.equals(DateTime.now.withTimeAtStartOfDay))
        assert(endDateRange.withTimeAtStartOfDay.equals(endDate.withTimeAtStartOfDay))
        assert(rangeCount == 1)
        assert(gapDaysNoStockpile == 0)
        assert(gapDaysStockpile == 0)
      }

      it("returns two separate date ranges for two date ranges that don\'t overlap with overlap of 30 days") {
        val firstDateStart = DateTime.now
        val firstDateEnd = DateTime.now
        val secondDateStart = DateTime.now.plusDays(100)
        val secondDateEnd = DateTime.now.plusDays(100)
        val metaList = Era.rangeBuilder(List((firstDateStart, firstDateEnd), (secondDateStart, secondDateEnd)))
        val ((firstStartDateRange, firstEndDateRange),
          firstRangeCount, firstGapDaysNoStockpile, firstGapDaysStockpile) = metaList.head
        val ((secondStartDateRange, secondEndDateRange),
          secondRangeCount, secondGapDaysNoStockpile, secondGapDaysStockpile) = metaList.last

        assert(metaList.length == 2)

        assert(firstStartDateRange.withTimeAtStartOfDay.equals(firstDateStart.withTimeAtStartOfDay))
        assert(firstEndDateRange.withTimeAtStartOfDay.equals(firstDateEnd.withTimeAtStartOfDay))
        assert(firstRangeCount == 1)
        assert(firstGapDaysNoStockpile == 0)
        assert(firstGapDaysStockpile == 0)

        assert(secondStartDateRange.withTimeAtStartOfDay.equals(secondDateStart.withTimeAtStartOfDay))
        assert(secondEndDateRange.withTimeAtStartOfDay.equals(secondDateEnd.withTimeAtStartOfDay))
        assert(secondRangeCount == 1)
        assert(secondGapDaysNoStockpile == 0)
        assert(secondGapDaysStockpile == 0)
      }

      it("returns one extended date range for date ranges that overlap by date overlap logically only") {
        val firstDateStart = DateTime.now
        val firstDateEnd = DateTime.now
        // https://stackoverflow.com/questions/26516765/java-joda-time-api-compare-intervals-detect-overlapping-and-generate-new-int
        // inclusive first date, exclusive second
        val plusDays = 29
        val secondDateStart = DateTime.now.plusDays(plusDays)
        val secondDateEnd = DateTime.now.plusDays(plusDays)
        val metaList = Era.rangeBuilder(List((firstDateStart, firstDateEnd), (secondDateStart, secondDateEnd)))
        val ((firstStartDateRange, firstEndDateRange),
          firstRangeCount, firstGapDaysNoStockpile, firstGapDaysStockpile) = metaList.head

        assert(metaList.length == 1)

        assert(firstStartDateRange.withTimeAtStartOfDay.equals(firstDateStart.withTimeAtStartOfDay))
        assert(firstEndDateRange.withTimeAtStartOfDay.equals(secondDateEnd.withTimeAtStartOfDay))
        assert(firstRangeCount == 2)
        assert(firstGapDaysNoStockpile == plusDays)
        assert(firstGapDaysStockpile == plusDays)
      }

      it("returns one extended date range for date ranges that don\'t overlap in real time, not logically") {
        val firstDateStart = DateTime.now.withTimeAtStartOfDay
        val firstDateEnd = DateTime.now.withTimeAtStartOfDay
        val plusDays = 29
        val secondDateStart = DateTime.now.withTimeAtStartOfDay
        val secondDateEnd = DateTime.now.withTimeAtStartOfDay.plusDays(plusDays)
        val metaList = Era.rangeBuilder(List((firstDateStart, firstDateEnd), (secondDateStart, secondDateEnd)))
        val ((firstStartDateRange, firstEndDateRange),
          firstRangeCount, firstGapDaysNoStockpile, firstGapDaysStockpile) = metaList.head

        assert(metaList.length == 1)
        assert(firstStartDateRange.withTimeAtStartOfDay.equals(firstDateStart.withTimeAtStartOfDay))
        assert(firstEndDateRange.withTimeAtStartOfDay.equals(secondDateEnd.withTimeAtStartOfDay))
        assert(firstRangeCount == 2)
        assert(firstGapDaysNoStockpile == 0)
        // overlap 1 day start
        assert(firstGapDaysStockpile == -1)
      }

      it("returns three separate date ranges for three date ranges that don\t overlap") {
        val firstDateStart = DateTime.now
        val firstDateEnd = DateTime.now
        val secondDateStart = DateTime.now.plusDays(40)
        val secondDateEnd = DateTime.now.plusDays(40)
        val thirdDateStart = DateTime.now.plusDays(80)
        val thirdDateEnd = DateTime.now.plusDays(80)

        val metaList = Era.rangeBuilder(
          List((firstDateStart, firstDateEnd), (secondDateStart, secondDateEnd), (thirdDateStart, thirdDateEnd))
        )
        val ((firstStartDateRange, firstEndDateRange),
          firstRangeCount, firstGapDaysNoStockpile, firstGapDaysStockpile) = metaList.head
        val ((secondStartDateRange, secondEndDateRange),
          secondRangeCount, secondGapDaysNoStockpile, secondGapDaysStockpile) = metaList.last

        assert(metaList.length == 3)

        assert(firstStartDateRange.withTimeAtStartOfDay().equals(firstDateStart.withTimeAtStartOfDay()))
        assert(firstEndDateRange.withTimeAtStartOfDay().equals(firstDateEnd.withTimeAtStartOfDay()))
        assert(firstRangeCount == 1)
        assert(firstGapDaysNoStockpile == 0)
        assert(firstGapDaysStockpile == 0)

        assert(secondStartDateRange.withTimeAtStartOfDay().equals(secondDateStart.withTimeAtStartOfDay()))
        assert(secondEndDateRange.withTimeAtStartOfDay().equals(secondDateEnd.withTimeAtStartOfDay()))
        assert(secondRangeCount == 1)
        assert(secondGapDaysNoStockpile == 0)
        assert(secondGapDaysStockpile == 0)
      }

      it("returns two date ranges for two ranges that overlap and one that doesn\'t") {

      }

    }

  }
}
