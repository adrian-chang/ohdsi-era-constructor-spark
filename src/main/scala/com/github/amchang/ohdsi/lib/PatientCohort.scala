package com.github.amchang.ohdsi.lib

/**
  * Reproduce the get PatientCohort Function
  */
object PatientCohort extends Spark {

  def build() = {
  /*  patients_list_df[[1]] <- executeSQL(connection, schema, paste("SELECT distinct(person_id) FROM @cdmSchema.observation WHERE observation_concept_id IN " +
      "(", paste(includeConceptlist,collapse=","), ") AND observation_concept_id NOT IN (", paste(excludeConceptlist,collapse=","), ") AND qualifier_concept_id=0;",sep=''),dbms)


    patients_list_df[[2]] <- executeSQL(connection, schema, paste("SELECT distinct(person_id) FROM @cdmSchema.condition_occurrence WHERE condition_concept_id " +
      "IN (",paste(includeConceptlist,collapse=","), ") AND condition_concept_id NOT IN (", paste(excludeConceptlist,collapse=","), ");", sep=''),dbms)

    casesANDcontrols_df[[2]] <- executeSQL(connection, schema, paste("SELECT person_id FROM (SELECT TM.person_id, ROW_NUMBER() OVER (ORDER BY RAND()) AS rn" +
      " FROM (SELECT A.person_id FROM @cdmSchema.person A LEFT JOIN ( (SELECT distinct(person_id) FROM @cdmSchema.observation WHERE observation_concept_id NOT IN " +
      "(", paste(excludeConceptlist,collapse=","), ") AND observation_concept_id IN (", paste(includeConceptlist,collapse=","), ") AND qualifier_concept_id=0)  " +
      "UNION (SELECT distinct(person_id) FROM @cdmSchema.condition_occurrence WHERE condition_concept_id NOT IN (", paste(excludeConceptlist,collapse=","), ") " +
      "AND condition_concept_id IN (", paste(includeConceptlist,collapse=","), "))) B ON A.person_id=B.person_id WHERE B.person_id IS NULL) TM) tmp WHERE rn <= ",controlSize,";" ,sep=''),dbms)*/


  }

  private def observationConceptList() = {

  }

  private def conditionOccurrenceList() = {

  }

}
