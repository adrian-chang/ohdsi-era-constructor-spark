package com.github.amchang.ohdsi.lib

/**
  * Created by achang on 4/2/16.
  */
object PatientData {

  def build = {
   /* patientFeatures_drugexposures_df<- list()
    patientFeatures_observations_df<- list()
    patientFeatures_visits_df<- list()
    patientFeatures_labs_df<- list()

    # domains that we do not want to include as features
    #removeDomains <- flags$remove_domains[1]

    for (patientQueue in 1:(length(patient_ids))) {

      patients_list_df<- list()

      # get patient dates
      patients_list_df[[1]] <- executeSQL(connection, schema, paste("SELECT person_id, observation_date FROM @cdmSchema.observation WHERE qualifier_concept_id=0 AND person_id=",as.character(patient_ids[patientQueue]),";",sep=''),dbms)
      patients_list_df[[2]] <- executeSQL(connection, schema, paste("SELECT person_id, condition_start_date AS observation_date FROM @cdmSchema.condition_occurrence WHERE person_id=",as.character(patient_ids[patientQueue]),";",sep=''),dbms)
      dates <- do.call(rbind, patients_list_df)
      remove('patients_list_df')

      # get normalization term
      timeDiff <- getNormalizationTerm(dates, flags)

      if (flags$drugexposures[1]) {
        if (removeDomains=='') { #No need to filter by domains if not present
          tmp_fv = executeSQL(connection, schema, paste("SELECT A.drug_exposure_id, A.person_id, A.drug_concept_id as concept_id, A.drug_exposure_start_date as feat_date, A.drug_type_concept_id, A.stop_reason, B.concept_name FROM @cdmSchema.drug_exposure A, @cdmSchema.concept B WHERE A.person_id=",as.character(patient_ids[patientQueue])," AND A.drug_concept_id=B.concept_id AND B.standard_concept='S' AND B.invalid_reason IS NULL AND B.concept_id NOT IN (", paste(keywords,collapse=","), ");",sep=''), dbms)
        } else {
          tmp_fv = executeSQL(connection, schema, paste("SELECT A.drug_exposure_id, A.person_id, A.drug_concept_id as concept_id, A.drug_exposure_start_date as feat_date, A.drug_type_concept_id, A.stop_reason, B.concept_name FROM @cdmSchema.drug_exposure A, @cdmSchema.concept B WHERE A.person_id=",as.character(patient_ids[patientQueue])," AND A.drug_concept_id=B.concept_id AND B.standard_concept='S' AND B.invalid_reason IS NULL  AND B.domain_id NOT IN (", paste(removeDomains,collapse=","), ") AND B.concept_id NOT IN (", paste(keywords,collapse=","), ");",sep=''), dbms)
        }
        test1 <- manipulateSqlPull(tmp_fv, flags, timeDiff)
        row.names(test1)<-as.character(patient_ids[patientQueue])
        patientFeatures_drugexposures_df[[patientQueue]]<-test1   #Assign the already transformed FV
        rm('test1')
        rm('tmp_fv')
      }
      if (flags$observations[1]) {
        if (removeDomains=='') { #No need to filter by domains if not present
          tmp_fv = executeSQL(connection, schema, paste("SELECT A.observation_id, A.person_id, A.observation_concept_id as concept_id, A.observation_date as feat_date, A.observation_type_concept_id, B.concept_name, B.domain_id FROM @cdmSchema.observation A, @cdmSchema.concept B WHERE A.person_id=",as.character(patient_ids[patientQueue])," AND A.qualifier_concept_id = 0 AND A.observation_concept_id=B.concept_id AND B.standard_concept='S' AND B.invalid_reason IS NULL AND B.concept_id NOT IN (", paste(keywords,collapse=","), ");",sep=''), dbms)
        } else {
          tmp_fv = executeSQL(connection, schema, paste("SELECT A.observation_id, A.person_id, A.observation_concept_id as concept_id, A.observation_date as feat_date, A.observation_type_concept_id, B.concept_name, B.domain_id FROM @cdmSchema.observation A, @cdmSchema.concept B WHERE A.person_id=",as.character(patient_ids[patientQueue])," AND A.qualifier_concept_id = 0 AND A.observation_concept_id=B.concept_id AND B.standard_concept='S' AND B.invalid_reason IS NULL AND B.domain_id NOT IN (", paste(removeDomains,collapse=","), ") AND B.concept_id NOT IN (", paste(keywords,collapse=","), ");",sep=''), dbms)
        }
        test1 <- manipulateSqlPull(tmp_fv, flags, timeDiff)
        row.names(test1)<-as.character(patient_ids[patientQueue])
        patientFeatures_observations_df[[patientQueue]]<-test1
        rm('test1')
        rm('tmp_fv')

      }
      if (flags$visits[1]) {
        if (removeDomains=='') { #No need to filter by domains if not present
          tmp_fv = executeSQL(connection, schema, paste("SELECT A.visit_occurrence_id, A.person_id, A.visit_start_date as feat_date, A.visit_end_date, B.condition_occurrence_id, B.condition_concept_id as concept_id, C.concept_name FROM @cdmSchema.visit_occurrence A, @cdmSchema.condition_occurrence B, @cdmSchema.concept C WHERE A.visit_occurrence_id = B.visit_occurrence_id AND A.person_id=",as.character(patient_ids[patientQueue])," AND B.condition_concept_id=C.concept_id AND C.standard_concept='S' AND C.invalid_reason IS NULL  AND C.concept_id NOT IN (", paste(keywords,collapse=","), ");",sep=''), dbms)
        } else {
          tmp_fv = executeSQL(connection, schema, paste("SELECT A.visit_occurrence_id, A.person_id, A.visit_start_date as feat_date, A.visit_end_date, B.condition_occurrence_id, B.condition_concept_id as concept_id, C.concept_name FROM @cdmSchema.visit_occurrence A, @cdmSchema.condition_occurrence B, @cdmSchema.concept C WHERE A.visit_occurrence_id = B.visit_occurrence_id AND A.person_id=",as.character(patient_ids[patientQueue])," AND B.condition_concept_id=C.concept_id AND C.standard_concept='S' AND C.invalid_reason IS NULL  AND C.domain_id NOT IN (", paste(removeDomains,collapse=","), ") AND C.concept_id NOT IN (", paste(keywords,collapse=","), ");",sep=''), dbms)
        }
        test1 <- manipulateSqlPull(tmp_fv, flags, timeDiff)
        row.names(test1)<-as.character(patient_ids[patientQueue])
        patientFeatures_visits_df[[patientQueue]]<-test1
        rm('test1')
        rm('tmp_fv')
      }
      if (flags$labs[1])  {
        tmp_fv = executeSQL(connection, schema, paste("SELECT A.measurement_id, A.person_id, A.measurement_date as feat_date, A.measurement_type_concept_id, A.measurement_concept_id, A.value_as_number, A.value_as_concept_id, B.concept_name FROM @cdmSchema.measurement A, @cdmSchema.concept B WHERE A.person_id=",as.character(patient_ids[patientQueue])," AND A.measurement_id NOT IN (", paste(keywords,collapse=","), ") AND A.measurement_concept_id=B.concept_id AND A.measurement_id NOT IN (", paste(keywords,collapse=","), ") AND A.measurement_concept_id!=0 AND A.measurement_concept_id!=4124462;", sep=''), dbms)
        tmp_fv$concept_id <- paste(tmp_fv$measurement_concept_id, tmp_fv$value_as_concept_id, sep=":")
        test1 <- manipulateSqlPull(tmp_fv, flags, timeDiff)
        row.names(test1)<-as.character(patient_ids[patientQueue])
        patientFeatures_labs_df[[patientQueue]]<-test1
        rm('test1')
        rm('tmp_fv')
      }
      #message(patientQueue)
    }
    patientData <- list(drugExposures = patientFeatures_drugexposures_df, observations = patientFeatures_observations_df, visits = patientFeatures_visits_df, labs = patientFeatures_labs_df)
    return (patientData)*/
  }

}
