package com.github.amchang.ohdsi.lib

import better.files.File
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * This is a shared base class for anyone that attempts to use the drug_condition table
  */
abstract class DrugStockpile extends Spark {

  type AncestorConceptId = Int
  type DescendantConceptId = Int
  type DrugExposureId = Int
  type PersonId = Int
  type DrugConceptId = Int
  type UnitConceptId = String
  type DoseValue = String
  type DrugExposureStartDate = DateTime
  type DaysSupply = String
  type DrugExposureEndDate = DateTime

  /**
    * Create the initial set of data to use for later merging
    *
    * @return the rdd to build eras from
    */
  protected val createInitialData = () => {
    val emptyDrugRecord = ((-1, -1, "", ""),  List[(DateTime, DateTime)]())
    val descendantConceptMappings = remainingConceptIds()
    val drugExposure = loadDrugExposure()
    val remainder = sparkContext.broadcast(descendantConceptMappings.toMap)

    val result = drugExposure.map { row =>
      val drugConceptId = row.getString(2).toInt
      val remainderMap = remainder.value

      // key safety
      if (remainderMap.contains(drugConceptId)) {
        val formatter = DateTimeFormat.forPattern("yyyyMMdd")
        val personId = row.getString(1).toInt
        val drugExposureStartDate = formatter.parseDateTime(row.getString(3))

        // deal with the end date, if no end date
        val daysSupply = row.getString(9)

        val drugExposureEndDate = if (row.getString(4).isEmpty) {
          if (daysSupply.isEmpty) {
            drugExposureStartDate.plusDays(1)
          } else {
            drugExposureStartDate.plusDays(daysSupply.toInt)
          }
        } else {
          formatter.parseDateTime(row.getString(4))
        }

        // following two can be blank, and there's no sql code substitution
        val unitConceptId = row.getString(13)
        val doseValue = row.getString(12)

        ((personId, remainderMap.get(drugConceptId).get, unitConceptId, doseValue),
          List((drugExposureStartDate, drugExposureEndDate)))
      } else {
        emptyDrugRecord
      }
    }.filter(_ != emptyDrugRecord).cache

    result
  } : RDD[((PersonId, DrugConceptId, UnitConceptId, DoseValue), List[(DrugExposureStartDate, DrugExposureEndDate)])]

  /**
    * Pre compute the ids we want from drug exposure or get them from a cache
    *
    * @return list of ids
    */
  private val remainingConceptIds = () => {
    import better.files._

    val file = File(getCacheFile("conceptIds"))
    val emptyConceptId = (-1, -1)
    var ids: List[(DescendantConceptId, AncestorConceptId)] = null

    // use the cache
    if (file.exists) {
      val sourceFile = sparkContext.objectFile[(DescendantConceptId, AncestorConceptId)](file.path.toString)

      ids = sourceFile.collect.toList
      // close the files
    } else {
      val conceptAncestor = loadConceptAncestor()
      val concept = loadConcept()

      // find the right values to go for
      val coMap: RDD[(Int, Int)] = concept.map { row =>
        val conceptId = row.getString(0).toInt
        val conceptClass = row.getString(4)
        val vocabularyId = row.getString(3)

        // id can be an int or alphanumeric, this is a bug in the main one, it uses '8' instead of RxNorm
        if (conceptClass == "Ingredient" && vocabularyId == "RxNorm") {
          (conceptId, conceptId)
        } else {
          emptyConceptId
        }
      }.filter(_ != emptyConceptId).cache

      // this should be small enough to broadcast
      val idsToRemain = sparkContext.broadcast(coMap.collect.toMap)

      // map the join table
      val caMap = conceptAncestor.map { row =>
        val ancestorConceptId = row.getString(0).toInt
        val descendantConceptId: Int = row.getString(1).toInt

        if (idsToRemain.value.contains(ancestorConceptId)) {
          (descendantConceptId, ancestorConceptId)
        } else {
          emptyConceptId
        }
      }.filter(_ != emptyConceptId).cache

      caMap.saveAsObjectFile(file.path.toString)

      ids = caMap.collect.toList
    }

    ids
  } : List[(DescendantConceptId, AncestorConceptId)]

  /**
    * Fire up drug_exposure.csv
    *
    * @return DataFrame of the drug exposure
    */
  private val loadDrugExposure = () => {
    csvDataReader
      .load(getDataFile("CDM_DRUG_EXPOSURE.csv"))
      .cache
  } : DataFrame

  /**
    * Load up the concept_ancestor.csv
    *
    * @return DataFrame of concept ancestor
    */
  private val loadConceptAncestor = () => {
    csvVocabReader
      .load(getVocabFile("CONCEPT_ANCESTOR.csv"))
      .cache
  } : DataFrame

  /**
    * Load up the concept.csv
    *
    * @return data frame of concepts
    */
  private val loadConcept = () => {
    csvVocabReader
      .load(getVocabFile("CONCEPT.csv"))
      .cache
  } : DataFrame

}
