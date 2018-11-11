##Group 5 code  
##
##Group members: 
##  Sebastian Karg
##  Teresa Rozza
##  Lidia Martí
##  Ana Ochogavia
##  Cristina Fortia
##  Jordi Escuder
##  Lluís Blanch
##


# Install the "big R query" package, if neccessary by uncommenting the following two lines:
#install.packages('devtools')
#devtools::install_github("rstats-db/bigrquery")

library("bigrquery")

# Install ggplot2, uncomment next line if this is the first time this section is run.
# install.packages("ggplot2")
library("ggplot2")

# Re-install curl to avoid errors like:
# Error in curl::curl_fetch_memory(url, handle = handle) :
#   Error in the HTTP2 framing layer
# Uncomment next line if this is the first time this section is run.
# install.packages("curl")

# Install missing dependency, uncomment next line if this is the first time this section is run.
# install.packages("readr")

# Shared project.
project_id <- "datathon-tarragona-2018"
options(httr_oauth_cache=FALSE)

# Wrapper for running BigQuery queries.
run_query <- function(query){
  data <- query_exec(query, project=project_id, use_legacy_sql = FALSE)
  return(data)
}


#All Data relevant to our restrictions. People should be mechanically ventilated. 

#Merge patient data and subject data
# dod as dod_SSN / HOSP ? 
# vals =  PaCO2 values of each day 1 - 5 
#50816 FIO2
#50818 PCO2
#50821 PO2
all <- run_query('
                 WITH pdata AS(SELECT
                 icu.subject_id,
                 icu.icustay_id,
                 hw.Height,
                 vd.duration_hours,
                 pat.dob,
                 pat.dod,
                 pat.gender,
                 icu.outtime,
                 first.RespRate_Mean,
                 sofa.SOFA,
                 DATETIME_DIFF(icu.outtime, icu.intime, DAY) AS icu_length_of_stay,
                 DATE_DIFF(DATE(icu.intime), DATE(pat.dob), YEAR) AS age
                 FROM `physionet-data.mimiciii_clinical.icustays` AS icu
                 LEFT JOIN `physionet-data.mimiciii_derived.heightfirstday` AS hw
                 ON icu.icustay_id = hw.icustay_id
                 LEFT JOIN `physionet-data.mimiciii_derived.sofa` AS sofa
                 ON icu.icustay_id = sofa.icustay_id
                 LEFT JOIN `physionet-data.mimiciii_derived.vitalsfirstday` AS first
                 ON icu.icustay_id = first.icustay_id
                 LEFT JOIN `physionet-data.mimiciii_derived.ventdurations` AS vd
                 ON icu.icustay_id = vd.icustay_id
                 LEFT JOIN `physionet-data.mimiciii_clinical.patients` AS pat
                 ON icu.subject_id = pat.subject_id)
                 SELECT
                 pdata.*
                 FROM pdata
                 ORDER BY Height DESC
                 ')

#' --#d_items_label<-('INNER JOIN `physionet-data.mimiciii_clinical.d_items` d_items 
#'   --#ON chartsub.itemid = d_items.itemid
#'   --#WHERE itemid in (226743,227428,1448)) 
#'   #'
#'   #)
#'   
#'   #View(reshape(all, idvar = c("subject_id"     ,   "icustay_id"   ,      "Height"        ,     "duration_hours"  ,   "dob","dod"                ,"gender"             ,"outtime"           #,"icu_length_of_stay", "age"     ), timevar = 'label', direction = "wide"))
#'  


#Generate the data per day. For that run the following code with different time points to the ICU-Stay.

day1 <- run_query("
                  
                  with pvt as
                  ( -- begin query that extracts the data
                  select ie.subject_id, ie.hadm_id, ie.icustay_id
                  -- here we assign labels to ITEMIDs
                  -- this also fuses together multiple ITEMIDs containing the same data
                  , case
                  when itemid = 50813 then 'LACTATE'
                  when itemid = 50816 then 'FIO2'
                  when itemid = 50818 then 'PCO2'
                  when itemid = 50819 then 'PEEP'
                  when itemid = 50820 then 'PH'
                  when itemid = 50821 then 'PO2'
                  when itemid = 50826 then 'TIDALVOLUME'
                  when itemid = 50827 then 'VENTILATIONRATE'
                  else null
                  end as label
                  , charttime
                  , value
                  -- add in some sanity checks on the values
                  , case
                  when valuenum <= 0 then null
                  when itemid = 50810 and valuenum > 100 then null -- hematocrit
                  -- ensure FiO2 is a valid number between 21-100
                  -- mistakes are rare (<100 obs out of ~100,000)
                  -- there are 862 obs of valuenum == 20 - some people round down!
                  -- rather than risk imputing garbage data for FiO2, we simply NULL invalid values
                  when itemid = 50816 and valuenum < 20 then null
                  when itemid = 50816 and valuenum > 100 then null
                  when itemid = 50817 and valuenum > 100 then null -- O2 sat
                  when itemid = 50815 and valuenum >  70 then null -- O2 flow
                  when itemid = 50821 and valuenum > 800 then null -- PO2
                  -- conservative upper limit
                  else valuenum
                  end as valuenum
                  
                  FROM `physionet-data.mimiciii_clinical.icustays` ie
                  left join `physionet-data.mimiciii_clinical.labevents` le
                  on le.subject_id = ie.subject_id and le.hadm_id = ie.hadm_id
                  -- adjust time according to desired time intervals 
                  and le.charttime between (DATETIME_SUB(ie.intime , INTERVAL 6 HOUR)) and (DATETIME_ADD(ie.intime , INTERVAL 1 DAY))
                  and le.ITEMID in
                  -- blood gases
                  (
                  50800, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809
                  , 50810, 50811, 50812, 50813, 50814, 50815, 50816, 50817, 50818, 50819
                  , 50820, 50821, 50822, 50823, 50824, 50825, 50826, 50827, 50828
                  , 51545
                  )
                  )
                  select pvt.SUBJECT_ID, pvt.HADM_ID, pvt.ICUSTAY_ID, pvt.CHARTTIME
                  
                  , max(case when label = 'LACTATE' then valuenum else null end) as LACTATE
                  , max(case when label = 'FIO2' then valuenum else null end) as FIO2
                  , max(case when label = 'PCO2' then valuenum else null end) as PCO2
                  , max(case when label = 'PEEP' then valuenum else null end) as PEEP
                  , max(case when label = 'PH' then valuenum else null end) as PH
                  , max(case when label = 'PO2' then valuenum else null end) as PO2
                  , max(case when label = 'TIDALVOLUME' then valuenum else null end) as TIDALVOLUME
                  , max(case when label = 'VENTILATIONRATE' then valuenum else null end) as VENTILATIONRATE
                  from pvt
                  group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME
                  order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME;
                  
                  ")



week <- run_query("
                  
                  with pvt as
                  ( -- begin query that extracts the data
                  select ie.subject_id, ie.hadm_id, ie.icustay_id
                  -- here we assign labels to ITEMIDs
                  -- this also fuses together multiple ITEMIDs containing the same data
                  , case
                  when itemid = 50813 then 'LACTATE'
                  when itemid = 50816 then 'FIO2'
                  when itemid = 50818 then 'PCO2'
                  when itemid = 50819 then 'PEEP'
                  when itemid = 50820 then 'PH'
                  when itemid = 50821 then 'PO2'
                  when itemid = 50826 then 'TIDALVOLUME'
                  when itemid = 50827 then 'VENTILATIONRATE'
                  else null
                  end as label
                  , charttime
                  , value
                  -- add in some sanity checks on the values
                  , case
                  when valuenum <= 0 then null
                  when itemid = 50810 and valuenum > 100 then null -- hematocrit
                  -- ensure FiO2 is a valid number between 21-100
                  -- mistakes are rare (<100 obs out of ~100,000)
                  -- there are 862 obs of valuenum == 20 - some people round down!
                  -- rather than risk imputing garbage data for FiO2, we simply NULL invalid values
                  when itemid = 50816 and valuenum < 20 then null
                  when itemid = 50816 and valuenum > 100 then null
                  when itemid = 50817 and valuenum > 100 then null -- O2 sat
                  when itemid = 50815 and valuenum >  70 then null -- O2 flow
                  when itemid = 50821 and valuenum > 800 then null -- PO2
                  -- conservative upper limit
                  else valuenum
                  end as valuenum
                  
                  FROM `physionet-data.mimiciii_clinical.icustays` ie
                  left join `physionet-data.mimiciii_clinical.labevents` le
                  on le.subject_id = ie.subject_id and le.hadm_id = ie.hadm_id
                  and le.charttime between (DATETIME_SUB(ie.intime , INTERVAL 6 HOUR)) and (DATETIME_ADD(ie.intime , INTERVAL 7 DAY))
                  and le.ITEMID in
                  -- blood gases
                  (
                  50800, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809
                  , 50810, 50811, 50812, 50813, 50814, 50815, 50816, 50817, 50818, 50819
                  , 50820, 50821, 50822, 50823, 50824, 50825, 50826, 50827, 50828
                  , 51545
                  )
                  )
                  select pvt.SUBJECT_ID, pvt.HADM_ID, pvt.ICUSTAY_ID, pvt.CHARTTIME
                  
                  , max(case when label = 'LACTATE' then valuenum else null end) as LACTATE
                  , max(case when label = 'FIO2' then valuenum else null end) as FIO2
                  , max(case when label = 'PCO2' then valuenum else null end) as PCO2
                  , max(case when label = 'PEEP' then valuenum else null end) as PEEP
                  , max(case when label = 'PH' then valuenum else null end) as PH
                  , max(case when label = 'PO2' then valuenum else null end) as PO2
                  , max(case when label = 'TIDALVOLUME' then valuenum else null end) as TIDALVOLUME
                  , max(case when label = 'VENTILATIONRATE' then valuenum else null end) as VENTILATIONRATE
                  from pvt
                  group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME
                  order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME;
                  
                  ")

#Median of data
tmp <- week
week_val = tmp %>% group_by(ICUSTAY_ID) %>% summarise(FIO2 = median(FIO2, na.rm = TRUE),PO2 = median(PO2, na.rm = TRUE),PCO2 = median(PCO2, na.rm = TRUE),Lactate = median(LACTATE, na.rm = TRUE),Tidal = median(TIDALVOLUME, na.rm = TRUE),PH = median(PH, na.rm = TRUE),PEEP = median(PEEP, na.rm = TRUE),VR = median(VENTILATIONRATE, na.rm = TRUE))

colnames(week_val)[1] <- "icustay_id"
allmerged_week = left_join(all,week_val,"icustay_id")
allmerged_week <- allmerged_week[(which(!is.na(allmerged_week$Height))),]

allmerged_week$Vent_1 <-  ifelse(allmerged_week$gender=="M",((allmerged_week$Tidal/allmerged_week$RespRate_Mean)*allmerged_week$PCO2)/((50+(0.91*allmerged_week$Height-152.4))*100*37.5),((allmerged_week$Tidal/allmerged_week$RespRate_Mean)*allmerged_week$PCO2)/((45.5+(0.91*allmerged_week$Height-152.4))*100*37.5))
allmerged_week$pafi <- allmerged_week$PO2 / allmerged_week$FIO2
dead <- which(substr(allmerged_week$dod,0,10)==substr(allmerged_week$outtime,0,10))
allmerged_week$deadlist <- 0
allmerged_week$deadlist[dead] <- 1


tmp <- day1
day_1 = tmp %>% group_by(ICUSTAY_ID) %>% summarise(FIO2_1 = median(FIO2, na.rm = TRUE),PO2_1 = median(PO2, na.rm = TRUE),PCO2_1 = median(PCO2, na.rm = TRUE),Lactate_1 = median(LACTATE, na.rm = TRUE),Tidal_1 = median(TIDALVOLUME, na.rm = TRUE),PH_1 = median(PH, na.rm = TRUE),PEEP_1 = median(PEEP, na.rm = TRUE),VR_1 = median(VENTILATIONRATE, na.rm = TRUE)) 

tmp <- day2
day_2 = tmp %>% group_by(ICUSTAY_ID) %>% summarise(FIO2_2 = median(FIO2, na.rm = TRUE),PO2_2 = median(PO2, na.rm = TRUE),PCO2_2 = median(PCO2, na.rm = TRUE),Lactate_2 = median(LACTATE, na.rm = TRUE),Tidal_2 = median(TIDALVOLUME, na.rm = TRUE),PH_2 = median(PH, na.rm = TRUE),PEEP_2 = median(PEEP, na.rm = TRUE),VR_2 = median(VENTILATIONRATE, na.rm = TRUE))

tmp <- day3
day_3 = tmp %>% group_by(ICUSTAY_ID) %>% summarise(FIO2_3 = median(FIO2, na.rm = TRUE),PO2_3 = median(PO2, na.rm = TRUE),PCO2_3 = median(PCO2, na.rm = TRUE),Lactate_3 = median(LACTATE, na.rm = TRUE),Tidal_3 = median(TIDALVOLUME, na.rm = TRUE),PH_3 = median(PH, na.rm = TRUE),PEEP_3 = median(PEEP, na.rm = TRUE),VR_3 = median(VENTILATIONRATE, na.rm = TRUE))

tmp <- day4
day_4 = tmp %>% group_by(ICUSTAY_ID) %>% summarise(FIO2_4 = median(FIO2, na.rm = TRUE),PO2_4 = median(PO2, na.rm = TRUE),PCO2_4 = median(PCO2, na.rm = TRUE),Lactate_4 = median(LACTATE, na.rm = TRUE),Tidal_4 = median(TIDALVOLUME, na.rm = TRUE),PH_4 = median(PH, na.rm = TRUE),PEEP_4 = median(PEEP, na.rm = TRUE),VR_4 = median(VENTILATIONRATE, na.rm = TRUE))

tmp <- day5
day_5 = tmp %>% group_by(ICUSTAY_ID) %>% summarise(FIO2_5 = median(FIO2, na.rm = TRUE),PO2_5 = median(PO2, na.rm = TRUE),PCO2_5 = median(PCO2, na.rm = TRUE),Lactate_5 = median(LACTATE, na.rm = TRUE),Tidal_5 = median(TIDALVOLUME, na.rm = TRUE),PH_5 = median(PH, na.rm = TRUE),PEEP_5 = median(PEEP, na.rm = TRUE),VR_5 = median(VENTILATIONRATE, na.rm = TRUE))


val <- left_join(left_join(left_join(left_join(day_1,day_2,"ICUSTAY_ID"),day_3,"ICUSTAY_ID"),day_4,"ICUSTAY_ID"),day_5,"ICUSTAY_ID")

#val$FIO2 <- median(val$FIO2_1,val$FIO2_2,val$FIO2_3,val$FIO2_4,val$FIO2_5, na.rm=TRUE) 
#val$PO2 <- median(val$PO2_1,val$PO2_2,val$PO2_3,val$PO2_4,val$PO2_5, na.rm=TRUE) 
#val$FIO2 <- median(val$FIO2_1,val$FIO2_2,val$FIO2_3,val$FIO2_4,val$FIO2_5, na.rm=TRUE) 


#all , val 
#tmp <- all 
#val2 <- as.data.frame(val)
colnames(val2)[1] <- "icustay_id"
allmerged = left_join(all,val2,"icustay_id")
allmerged <- allmerged[(which(!is.na(allmerged$Height))),]
#allmerged <- allmerged[(which(!is.na(allmerged$Height))),]
allmerged$Vent_1 <-  ifelse(allmerged$gender=="M",((allmerged$Tidal_1/allmerged$RespRate_Mean)*allmerged$PCO2_1)/((50+(0.91*allmerged$Height-152.4))*100*37.5),((allmerged$Tidal_1/allmerged$RespRate_Mean)*allmerged$PCO2_1)/((45.5+(0.91*allmerged$Height-152.4))*100*37.5))
allmerged$Vent_2 <-  ifelse(allmerged$gender=="M",((allmerged$Tidal_2/allmerged$RespRate_Mean)*allmerged$PCO2_2)/((50+(0.91*allmerged$Height-152.4))*100*37.5),((allmerged$Tidal_2/allmerged$RespRate_Mean)*allmerged$PCO2_2)/((45.5+(0.91*allmerged$Height-152.4))*100*37.5))
allmerged$Vent_3 <-  ifelse(allmerged$gender=="M",((allmerged$Tidal_3/allmerged$RespRate_Mean)*allmerged$PCO2_3)/((50+(0.91*allmerged$Height-152.4))*100*37.5),((allmerged$Tidal_3/allmerged$RespRate_Mean)*allmerged$PCO2_3)/((45.5+(0.91*allmerged$Height-152.4))*100*37.5))
allmerged$Vent_4 <-  ifelse(allmerged$gender=="M",((allmerged$Tidal_4/allmerged$RespRate_Mean)*allmerged$PCO2_4)/((50+(0.91*allmerged$Height-152.4))*100*37.5),((allmerged$Tidal_4/allmerged$RespRate_Mean)*allmerged$PCO2_4)/((45.5+(0.91*allmerged$Height-152.4))*100*37.5))
allmerged$Vent_5 <-  ifelse(allmerged$gender=="M",((allmerged$Tidal_5/allmerged$RespRate_Mean)*allmerged$PCO2_5)/((50+(0.91*allmerged$Height-152.4))*100*37.5),((allmerged$Tidal_5/allmerged$RespRate_Mean)*allmerged$PCO2_5)/((45.5+(0.91*allmerged$Height-152.4))*100*37.5))

allmerged$pafi_1 <- allmerged$PO2_1 / allmerged$FIO2_1
allmerged$pafi_2 <- allmerged$PO2_2 / allmerged$FIO2_2
allmerged$pafi_3 <- allmerged$PO2_3 / allmerged$FIO2_3
allmerged$pafi_4 <- allmerged$PO2_4 / allmerged$FIO2_4
allmerged$pafi_5 <- allmerged$PO2_5 / allmerged$FIO2_5

dead <- which(substr(allmerged$dod,0,10)==substr(allmerged$outtime,0,10))
allmerged$deadlist <- 0
allmerged$deadlist[dead] <- 1

#VRmen = ((tidal/resp_R)*PCO2)/((50+(0.91*height-152.4))*100*37.5)
#VRwomen = ((tidal/resp_R)*PCO2)/((45.5+(0.91*height-152.4))*100*37.5)

# # Additional formulas to keep in mind
# VR = VM_measured * PaCO2_aterial / VM_pred * paco2_pred
# VM_pred_male = 50 + 0.91(height - 152.4)   * 100ml/min
# VM_pred_female = 45.5  + 0.91   (height - 152.2) * 100ml/min
# paco2_pred = 37.5 mm/H




#code for the whole week 

weekmerged_imputed <- allmerged_week[,c("Height","gender","SOFA","icu_length_of_stay",
                                "age","pafi","Vent_1","Lactate","PH","PEEP","deadlist")]

weekmerged_imputed <- weekmerged_imputed[which(!is.na(weekmerged_imputed$Vent_1)),]


mod_multiv_week<-glm(deadlist ~ 
                  gender
                +SOFA
                +Height
                +icu_length_of_stay
                +age
                +pafi
                +Vent_1
                +Lactate
                +PH
                +PEEP
                ,family='binomial'
                ,data = weekmerged_imputed
)


oddsratios_table<-exp(cbind(coef(mod_multiv_week), confint(mod_multiv_week)))
options(scipen=999)
oddsratios_table<-round(oddsratios_table,4)
#oddsratios_table['x']<-(1:10)

oddsratios_table<-as.data.frame(oddsratios_table)
colnames(oddsratios_table)[1]<-"OR"

oddsratios_table<-oddsratios_table[order(row.names(oddsratios_table)), ]
  

dead <- which(substr(allmerged$dod,0,10)==substr(allmerged$outtime,0,10))
allmerged$deadlist <- 0
allmerged$deadlist[dead] <- 1


#first day
relevant_data_imputed_1 <- allmerged[,c("gender","SOFA","icu_length_of_stay","age","pafi_1","Vent_1","deadlist")]


withvent <- relevant_data_imputed_1[which(!is.na(relevant_data_imputed_1$Vent_1)),]

mod_multiv<-glm(deadlist ~ 
                  gender
                +SOFA
                +icu_length_of_stay
                +age
                +pafi_1
                +Vent_1
                ,family='binomial'
                ,data = withvent
)

oddsratios_table<-exp(cbind(coef(mod_multiv), confint(mod_multiv)))
options(scipen=999)
oddsratios_table<-round(oddsratios_table,4)
#oddsratios_table['x']<-(1:10)

oddsratios_table<-as.data.frame(oddsratios_table)
colnames(oddsratios_table)[1]<-"OR"

oddsratios_table<-oddsratios_table[order(row.names(oddsratios_table)), ]


#Generate plots

df <- oddsratios_table


df <- data.frame(yAxis = 7:1,
                 boxOdds = oddsratios_table$OR,
                 boxCILow = oddsratios_table$`2.5 %`,  
                 boxCIHigh = oddsratios_table$`97.5 %`
)
#df <- df[order(df$boxOdds),]

ggplot(df, aes(x = boxOdds, y = rownames(oddsratios_table))) + 
  geom_vline(aes(xintercept = 0), size = .25, linetype = "dashed") + 
  geom_errorbarh(aes(xmax = boxCIHigh, xmin = boxCILow), size = .5, height = 
                   .2, color = "gray50") +
  geom_point(size = 3.5, color = "orange") +
  coord_trans(x = scales:::exp_trans(10)) +
  theme_bw()+
  theme(panel.grid.minor = element_blank()) +
  ylab("") +
  xlab("Odds ratio") +
  ggtitle("Impact on mortality")



