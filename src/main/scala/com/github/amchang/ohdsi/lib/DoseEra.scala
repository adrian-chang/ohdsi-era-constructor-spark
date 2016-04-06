package com.github.amchang.ohdsi.lib

import java.nio.file.{Files, Paths}

import com.github.nscala_time.time.Imports._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame



/**
  * Replication of functionality in
  *
  * https://github.com/OHDSI/Era-Constructor/blob/master/v5/PostgreSQL/postgres_v5_dose_era.sql
  */
object DoseEra extends Spark with Era {

  type AncestorConceptId = Int
  type DescendantConceptId = Int
  type DrugExposureId = Int
  type PersonId = Int
  type IngredientConceptId = Int
  type UnitConceptId = String
  type DoseValue = String
  type DrugExposureStartDate = DateTime
  type DaysSupply = String
  type DrugExposureEndDate = DateTime

  // use these for none values
  private val emptyConceptId = (-1, -1)
  private val emptyDrugRecord = (-1, -1, -1, "", "", null, "", null)

  /**
    * Build Dosage Eras
    */
  def build = {
    // the entire data
    val netData = createInitialData
  }

  /**
    * Create the initial set of data to use for later merging
    */
  private def createInitialData: RDD[
    (DrugExposureId, PersonId, IngredientConceptId, UnitConceptId, DoseValue,
      DrugExposureStartDate, DaysSupply, DrugExposureEndDate)
    ] = {
    val descendantConceptMappings = remainingConceptIds
    val drugExposure = loadDrugExposure
    val remainder = sparkContext.broadcast(descendantConceptMappings.toMap)

    val result = drugExposure.map { row =>
      val drugConceptId = row.getString(2).toInt
      val remainderMap = remainder.value

      // key safety
      if (remainderMap.contains(drugConceptId)) {
        val formatter = DateTimeFormat.forPattern("yyyyMMdd")

        val drugExposureId = row.getString(0).toInt
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
        val doseUnitConceptId = row.getString(13)
        val effectiveDrugDose = row.getString(12)

        (drugExposureId, personId, remainderMap.get(drugConceptId).get, doseUnitConceptId,
          effectiveDrugDose, drugExposureStartDate, daysSupply, drugExposureEndDate)
      } else {
        emptyDrugRecord
      }
    }.filter(_ != emptyDrugRecord).cache

    result
  }

  /**
    * Pre compute the ids we want from drug exposure or get them from a cache
    *
    * @return list of ids
    */
  private def remainingConceptIds: List[(DescendantConceptId, AncestorConceptId)] = {
    import better.files._

    val file = File(getCacheFile("conceptIds"))
    var ids: List[(DescendantConceptId, AncestorConceptId)] = null

    // use the cache
    if (file.exists) {
      val sourceFile = sparkContext.objectFile[(DescendantConceptId, AncestorConceptId)](file.path.toString)

      ids = sourceFile.collect.toList
      // close the files
    } else {
      val conceptAncestor = loadConceptAncestor
      val concept = loadConcept

      // find the right values to go for
      val coMap: RDD[(Int, Int)] = concept.map { row =>
        val conceptId = row.getString(0).toInt
        val conceptClass = row.getString(4)
        val vocabularyId = row.getString(3)

        // id can be an int or alphanumeric, this is a bug in the main one, it uses '8' instead of RxNorm
        if (conceptClass == "Ingredient" && vocabularyId == "RxNorm") {
          (conceptId, conceptId)
        } else {
          (-1, -1)
        }
      }.filter(_ != (-1, -1)).cache

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
  }

  /**
    * Fire up drug_exposure.csv
    *
    * @return data frame of the drug exposure
    */
  private def loadDrugExposure: DataFrame = {
    csvDataReader
      .load(getDataFile("CDM_DRUG_EXPOSURE.csv"))
      .cache
  }

  /**
    * Load up the concept_ancestor.csv
    *
    * @return dataframe of concept ancestor
    */
  private def loadConceptAncestor: DataFrame = {
    csvVocabReader
      .load(getVocabFile("CONCEPT_ANCESTOR.csv"))
      .cache
  }

  /**
    * Load up the concept.csv
    *
    * @return dataframe of concepts
    */
  private def loadConcept: DataFrame = {
    csvVocabReader
      .load(getVocabFile("CONCEPT.csv"))
      .cache
  }

}
