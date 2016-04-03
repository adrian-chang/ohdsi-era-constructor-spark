package com.github.amchang.ohdsi.lib
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Build the ignore and keyword list
  */
object KeywordList extends Spark {

  /**
    * Various definitions columns
    */
  type ConceptId = String
  type ConceptName = String
  type AncestorConceptId = String
  type DescendantConceptId = String
  type RelatedConceptId = String
  type RelatedConceptName = String

  /**
    * Hold a reference to the concept csv
    */
  private var conceptCsv: DataFrame = null

  /**
    * Build a keyword list and ignore items
    *
    * @param name the concept to build around, lowercase
    * @return Tuple of (keyword, ignoreItems)
    */
  def build(name: String): (Option[List[String]], Option[List[String]]) = {
    val lConceptName = name.toLowerCase
    var concepts = findConcept(lConceptName)

    // nothing
    if (concepts.isEmpty) {
      concepts = findConceptSynonym(lConceptName)

      if (concepts.isEmpty) {
        return (None, None)
      }
    }

    // either one cache
    concepts.cache

    findKeywordIgnoreList(concepts)
  }

  /**
    * Generate the the keyword and ignore lists
    *
    * @param concepts the concepts to build around
    * @return keyword and ignore lists
    */
  private def findKeywordIgnoreList(concepts: RDD[(ConceptId, ConceptName)]): (Option[List[String]], Option[List[String]]) = {
    val top = unionTop(concepts)

    /*val conceptsBroadcast = sparkContext.broadcast(concepts)
    val allConceptsBroadcast = sparkContext.broadcast(loadAllConcept())



    concepts.map {
      case (conceptId, conceptName) =>
        val localConcepts = conceptsBroadcast.value
        val localAllAncestors = allAncestorsBroadcast.value
        val localAllConcepts = allConceptsBroadcast.value

    }*/
   /* "SELECT ALT.concept_id, ALT.concept_name, ALT.related_concept_id, ALT.related_concept_name FROM" +
      " ( (SELECT A.concept_id, A.concept_name, B.descendant_concept_id as related_concept_id, C.concept_name as related_concept_name  " +
      "FROM (SELECT concept_id, concept_name FROM @cdmSchema.concept WHERE lower(concept_name) =lower('",currentConcept_name,"') AND standard_concept = 'S' AND invalid_reason IS NULL AND domain_id = 'Condition') A, " +
      "@cdmSchema.concept_ancestor B, (SELECT concept_id, concept_name FROM @cdmSchema.concept WHERE invalid_reason IS NULL AND domain_id = 'Condition') C

      WHERE A.concept_id = B.ancestor_concept_id AND C.concept_id = B.descendant_concept_id) " +
      "" +
      "" +
      "UNION (SELECT A.concept_id_1 as concept_id, C.concept_name, A.concept_id_2 as related_concept_id, " +
      "B.concept_name as related_concept_name FROM @cdmSchema.concept_relationship A, @cdmSchema.concept B, @cdmSchema.concept C WHERE concept_id_1 = ",currentConcept_id," AND A.invalid_reason IS NULL
    AND A.concept_id_2 = B.concept_id AND A.concept_id_1=C.concept_id AND B.domain_id = 'Condition' AND C.domain_id='Condition')) ALT ORDER BY ALT.related_concept_id;*/
    (None, None)
  }

  /**
    * Top half of the union
    *
    * @param concepts current set of concepts
    * @return Rdd[(ConceptId, ConceptName, DescendantConceptId, RelatedConceptName)]
    */
  private def unionTop(concepts: RDD[(ConceptId, ConceptName)]): RDD[(ConceptId, ConceptName, DescendantConceptId, RelatedConceptName)] = {
    val allAncestor = loadConceptAncestor()
    val allConcept = loadAllConcept()

    // join and map down
    val conceptAncestor = concepts.join(allAncestor).map {
      case (conceptId, (conceptName, descendantId)) =>
        println(descendantId, conceptId)
        (descendantId, (conceptId, conceptName))
    }

    val conceptAncestorAll = conceptAncestor.join(allConcept).map {
      case (descConceptId, ((conceptId, conceptName), relatedConceptName)) =>
        (conceptId, conceptName, descConceptId, relatedConceptName)
    }

    conceptAncestorAll.cache
  }

  /**
    * Bottom half of the union
    * @return
    */
  private def unionBottom(): RDD[(ConceptId, ConceptName, DescendantConceptId, RelatedConceptName)] = {
    val conceptRelationship = csvReader
      .load(getVocabDir("CONCEPT_RELATIONSHIP.csv"))

    val concept = loadAllConcept

    conceptRelationship.map { row =>
      val invalidReasonIsNull = row.getString(5).isEmpty

      if (invalidReasonIsNull) {
        row.getString(0)
      } else {
        null
      }
    }

    null
  }

  /**
    * Get all of the concept ancestors
    *
    * @return RDD of ancestorConceptId, DescendantConceptId
    */
  private def loadConceptAncestor(): RDD[(AncestorConceptId, DescendantConceptId)] = {
    val conceptAncestor = csvReader
      .load(getVocabDir("CONCEPT_ANCESTOR.csv"))

    // simple load
    conceptAncestor.map { row =>
      (row.getString(0).toLowerCase, row.getString(1).toLowerCase)
    }.cache
  }

  /**
    * Get all concepts with certain logc
    *
    * @return conceptId, conceptName RDD
    */
  private def loadAllConcept(): RDD[(ConceptId, ConceptName)] = {
    val concept = loadConceptCsv

    concept.map { row =>
      val conceptId = row.getString(0).toLowerCase
      val conceptName = row.getString(1).toLowerCase
      val invalidReasonIsNull = row.getString(9).isEmpty
      val domainId = row.getString(2).toLowerCase

      // inner 'c'
      if (invalidReasonIsNull && domainId == "condition") {
        (conceptId, conceptName)
      } else {
        null
      }
    }.filter(_ != null).cache
  }

  /**
    * Find a concept by id
    *
    * @param conceptName the concept to try and find
    * @return RDD[(conceptId, conceptName)]
    */
  private def findConcept(conceptName: String): RDD[(ConceptId, ConceptName)] = {
    val concept = loadConceptCsv
    val findName = sparkContext.broadcast(conceptName)

    // find the concept directly
    concept
      .map { row =>
        val conceptId = row.getString(0).toLowerCase
        val cName = row.getString(1).toLowerCase
        val sConcept = row.getString(5).toLowerCase
        val domainId = row.getString(2).toLowerCase
        val invalidReasonIsNull = row.getString(9).isEmpty

        // the logic
        if (cName == findName.value &&
            sConcept == "s" &&
            invalidReasonIsNull &&
            domainId == "condition") {
          (conceptId, cName)
        } else {
          null
        }
      }.filter(_ != null)
  }

  /**
    * Find a concept through the synonym table
    *
    * @param conceptName the concept to try and find by synonym name
    * @return RDD[(conceptId, conceptName)]
    */
  private def findConceptSynonym(conceptName: String): RDD[(ConceptId, ConceptName)] = {
    val conceptSynonym = csvReader
        .load(getVocabDir("CONCEPT_SYNONYM.csv"))

    val name = sparkContext.broadcast(conceptName)

    // find the concept directly
    val remainder = sparkContext.broadcast(conceptSynonym
      .map { row =>
        val conceptId = row.getString(0).toLowerCase
        val synonym = row.getString(1).toLowerCase

        if (synonym == name.value) {
          conceptId
        } else {
          null
        }
      }.filter(_ != null).collect())

    val concept = loadConceptCsv

    concept.map { row =>
      val conceptId = row.getString(0).toLowerCase
      val sConcept = row.getString(5).toLowerCase
      val domainId = row.getString(2).toLowerCase
      val invalidReasonIsNull = row.getString(9).isEmpty
      val cName = row.getString(1).toLowerCase

      // the logic
      if (sConcept == "s" &&
        invalidReasonIsNull &&
        remainder.value.contains(conceptId) &&
        domainId == "condition") {
        (conceptId, cName)
      } else {
        null
      }
    }.filter(_ != null)
  }

  /**
    * Load the concept csv if needed
    *
    * @return concept csv
    */
  private def loadConceptCsv: DataFrame = {
    if (conceptCsv == null) {
      conceptCsv = csvReader
        .load(getVocabDir("CONCEPT.csv"))

      conceptCsv.cache
    }

    conceptCsv
  }

}
