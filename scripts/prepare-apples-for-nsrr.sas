*******************************************************************************;
* Program           : prepare-apples-for-nsrr.sas
* Project           : National Sleep Research Resource (sleepdata.org)
* Author            : Michael Rueschman (mnr)
* Date Created      : 20220304
* Purpose           : Prepare APPLES data for posting on NSRR.
*******************************************************************************;

*******************************************************************************;
* establish options and libnames ;
*******************************************************************************;
  options nofmterr;
  data _null_;
    call symput("sasfiledate",put(year("&sysdate"d),4.)||put(month("&sysdate"d),z2.)||put(day("&sysdate"d),z2.));
  run;

  *project source datasets;
  libname appless "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20220301-kushida-apples\nsrr-prep\_source";
  %let applessource = \\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20220301-kushida-apples\nsrr-prep\_source;

  *output location for nsrr sas datasets;
  libname applesd "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20220301-kushida-apples\nsrr-prep\_datasets";
  libname applesa "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20220301-kushida-apples\nsrr-prep\_archive";

  *nsrr id location;
  *libname applesi "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20220301-kushida-apples\nsrr-prep\_ids";

  *set data dictionary version;
  %let version = 0.1.1.pre;

  *set nsrr csv release path;
  %let releasepath = \\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20220301-kushida-apples\nsrr-prep\_releases;

*******************************************************************************;
* create datasets ;
*******************************************************************************;
  proc import datafile="&applessource\APPLES Baseline Characteristics.csv"
    out=apples_bl_char_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_bl_char_in nodupkey;
    by appleid visit;
  run;

  proc import datafile="&applessource\APPLES WASI Baseline Data.csv"
    out=apples_wasi_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_wasi_in nodupkey;
    by appleid visit;
  run;

  proc import datafile="&applessource\APPLES BSRT PFN SWMT MWT PASAT Longitudinal Data.csv"
    out=apples_bsrt_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_bsrt_in nodupkey;
    by appleid visit;
  run;

  proc import datafile="&applessource\APPLES CPAP Adherence Longitudinal Data.csv"
    out=apples_cpap_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_cpap_in nodupkey;
    by appleid visit;
  run;

  proc import datafile="&applessource\APPLES ESS BP BMI Longitudinal Data.csv"
    out=apples_ess_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_ess_in nodupkey;
    by appleid visit;
  run;

  proc import datafile="&applessource\APPLES POMS SAQLI Longitudinal Data.csv"
    out=apples_poms_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_poms_in nodupkey;
    by appleid visit;
  run;

  proc import datafile="&applessource\APPLES PSG Longitudinal Data.csv"
    out=apples_psg_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_psg_in nodupkey;
    by appleid visit;
  run;

  proc import datafile="&applessource\APPLES PVT Longitudinal Data.csv"
    out=apples_pvt_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_pvt_in nodupkey;
    by appleid visit;
  run;

  proc import datafile="&applessource\APPLEID-OriginalFileID.csv"
    out=apples_original_file_id_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  proc sort data=apples_original_file_id_in nodupkey;
    by appleid;
  run;


  proc import datafile="&applessource\extracted_slptime.csv"
    out=apples_extracted_slptime
    dbms=csv
    replace;
    guessingrows=1100;
  run;

  proc sort data=apples_extracted_slptime nodupkey;
    by appleid;
  run;



  *merge datasets;
  data apples_in;
    length appleid $7. visitn 8. visit $4. fileid $25. site $6.;
    merge
      apples_bl_char_in
      apples_wasi_in
      apples_bsrt_in
      apples_cpap_in
      apples_ess_in
      apples_poms_in
      apples_psg_in
      apples_pvt_in
	  apples_extracted_slptime
      ;
    by appleid visit;

    *'empty' visit rows in BSRT and PVT datasets - remove these empty rows entirely;
    if visit = '' then delete;

  *remove unrealistic values for bmi;
    if bmiblquan = -704.47 then bmiblquan = .;
  if bmihl = -704.47 then bmihl = .;

    *create numbered visit variable for sorting in chronological order based on APPLES dictionary;
    if visit = 'BL' then visitn = 1;
    else if visit = 'CE' then visitn = 2;
    else if visit = 'DX' then visitn = 3;
    else if visit = 'CPAP' then visitn = 4;
    else if visit = '2M' then visitn = 5;
    else if visit = '4M' then visitn = 6;
    else if visit = '6M' then visitn = 7;

    *rename variables with number at start of variable name;
    rename _5yrwtgain20ormorelbshp = wtgain20ormorelbshp;

    *recode values with different letter casing;
    if offtreatmentorstudy = "Off-study" then offtreatmentorstudy = "Off-Study";
    if offtreatmentorstudy = "Off-treatment" then offtreatmentorstudy = "Off-Treatment";

    *drop unnecessary/extraneous/redundant variables;
    drop 
      id /* evidently a per-dataset row indicator */
      studytype /* all values 'PSG', not helpful */
    caindextstpsgcalc /* drop duplicated var*/
    hypindextstpsgcalc /* drop duplicated var*/
    maindextstpsgcalc /* drop duplicated var*/
    oaindextstpsgcalc /* drop duplicated var*/
    desatindexwake /* drop duplicated and less clearly defined var*/
    wkbefsleep /* drop duplicated var*/
    wkaftsleep /* drop duplicated var*/
    wkdursleep /* drop duplicated var*/
      ;
  run;

  proc sort data=apples_in;
    by appleid visitn;
  run;

  data apples_fileid_in;
    length appleid $7. visitn 8. visit $4. fileid $25. site $6.;
    merge 
      apples_in
      apples_original_file_id_in
      ;
    by appleid;
    
    if original_fileid ne . and visitn in (3) then fileid = "apples-" || compress(put(original_fileid,8.));

    drop original_fileid /* converted to 'fileid' */ ;
  run;

  proc sort data=apples_fileid_in;
    by appleid visitn;
  run;

  data apples_retain;
    set apples_fileid_in;
    by appleid;

    retain gender2 ethnicity2 site2 age_at_enrollment;
    if first.appleid then do;
      gender2 = gender;
      ethnicity2 = ethnicity;
      site2 = site;
      age_at_enrollment = age;
    end;
  run;

  data apples_nsrr;
    set apples_retain;

    gender = gender2;
    ethnicity = ethnicity2;
    site = site2;

    drop
      gender2
      ethnicity2
      site2
      ;
  run;

*******************************************************************************;


  /* checking;

  proc freq data=apples_nsrr;
    table SyncopeMedHxHP;
  run;

  proc freq data=apples_bl_char;
    table bmiblquan;
  run;

  proc sql;
    select appleid
    from apples_nsrr
    where . < currentsmoker < 0;
  quit;

  */

*******************************************************************************;
* create harmonized datasets ;
*******************************************************************************;
data apples_harmonized;
  length nsrrid $9. visitn 8. fileid $25. site $6. age_at_enrollment 8.;
  set apples_nsrr;

  nsrrid=appleid;

*demographics;
*age;
*use age;
  format nsrr_age 8.;
  if age gt 89 then nsrr_age = 90;
  else if age le 89 then nsrr_age = age;

*age_gt89;
*use age;
  format nsrr_age_gt89 $100.; 
  if age gt 89 then nsrr_age_gt89='yes';
  else if age le 89 then nsrr_age_gt89='no';

*sex;
*use gender;
  format nsrr_sex $100.;
  if gender = '0) male' then nsrr_sex = 'male';
  else if gender = '1) female' then nsrr_sex = 'female';
  else if gender = '.' then nsrr_sex = 'not reported';

*race;
*use ethnicity;
    format nsrr_race $100.;
    if ethnicity = '0) Native American' then nsrr_race = 'american indian or alaska native';
    else if ethnicity = '1) Asian' then nsrr_race = 'asian';
    else if ethnicity = '2) Black' then nsrr_race = 'black or african american';
    else if ethnicity = '3) Hispanic' then nsrr_race = 'hispanic';
  else if ethnicity = '4) White' then nsrr_race = 'white';
  else if ethnicity = '5) Other' then nsrr_race = 'other';
  else  nsrr_race = 'not reported';

drop ethnicity;

*anthropometry;
*bmi;
*use bmiblquan;
  format nsrr_bmi 10.9;
  if bmiblquan gt 0 then nsrr_bmi = bmiblquan;
  else if age le 0 then nsrr_bmi = .;

*clinical data/vital signs;
*bp1a_am_recode;
*recode bp1a_am;
  format bp1a_am_recode 10.9;
  if bp1a_am gt 0 then bp1a_am_recode = bp1a_am;
  else if bp1a_am le 0 then bp1a_am_recode = .;

*bp2a_am_recode;
*recode bp2a_am;
  format bp2a_am_recode 10.9;
  if bp2a_am gt 0 then bp2a_am_recode = bp2a_am;
  else if bp2a_am le 0 then bp2a_am_recode = .;

*bp1b_am_recode;
*recode bp1b_am;
  format bp1b_am_recode 10.9;
  if bp1b_am gt 0 then bp1b_am_recode = bp1b_am;
  else if bp1b_am le 0 then bp1b_am_recode = .;

*bp2b_am_recode;
*recode bp2b_am;
  format bp2b_am_recode 10.9;
  if bp2b_am gt 0 then bp2b_am_recode = bp2b_am;
  else if bp2b_am le 0 then bp2b_am_recode = .;
  
*use bp1a_am_recode,bp2a_am_recode;
  format nsrr_bp_systolic 8.2;
  nsrr_bp_systolic = mean(bp1a_am_recode,bp2a_am_recode);

*bp_diastolic;
*use bp1b_am_recode,bp2b_am_recode;
  format nsrr_bp_diastolic 8.2;
  nsrr_bp_diastolic = mean(bp1b_am_recode,bp2b_am_recode);

*lifestyle and behavioral health;

*ever_smoker;
*use smokermedhxhp;
  format nsrr_ever_smoker $100.;
    if smokermedhxhp = '1' then nsrr_ever_smoker = 'yes';
    else if smokermedhxhp = '2' then nsrr_ever_smoker = 'yes';
    else if smokermedhxhp = '0'  then nsrr_ever_smoker = 'no';
    else if smokermedhxhp = '-1'  then nsrr_ever_smoker = 'not reported';
  else if smokermedhxhp = '-2'  then nsrr_ever_smoker = 'not reported';
  else nsrr_ever_smoker = 'not reported';

*current_smoker;
*use currentsmoker;
  format nsrr_current_smoker $100.;
    if currentsmoker = '1' then nsrr_current_smoker = 'yes';
    else if currentsmoker = '0' then nsrr_current_smoker = 'no';
  else if smokermedhxhp = '2' then nsrr_current_smoker = 'yes';
  else if smokermedhxhp = '1' then nsrr_current_smoker = 'no';
  else if smokermedhxhp = '0' then nsrr_current_smoker = 'no';
    else if currentsmoker = '-1' then nsrr_current_smoker = 'not reported';
    else if currentsmoker = '-2' then nsrr_current_smoker = 'not reported';
  else if currentsmoker = .  then nsrr_current_smoker = 'not reported';

*polysomnography;
*nsrr_ahi_chicago1999;
*use rditstpsg;
  format nsrr_ahi_chicago1999 8.2;
  if rditstpsg ge 0 then nsrr_ahi_chicago1999 = rditstpsg;
  else if rditstpsg lt 0 then nsrr_ahi_chicago1999 = .;

*nsrr_ttldursp_f1;
*use tstpsg;
  format nsrr_ttldursp_f1 8.2;
  if tstpsg ge 0 then nsrr_ttldursp_f1 = tstpsg;
  else if tstpsg lt 0 then nsrr_ttldursp_f1 = .;
  
*nsrr_phrnumar_f1;
*use arousalindexcalc;
  format nsrr_phrnumar_f1 8.2;
  if arousalindexcalc ge 0 then nsrr_phrnumar_f1 = arousalindexcalc;
  else if arousalindexcalc lt 0 then nsrr_phrnumar_f1 = .;
  
*nsrr_ttleffsp_f1;
*use sleepeffpsg;
  format nsrr_ttleffsp_f1 8.2;
  if sleepeffpsg ge 0 then nsrr_ttleffsp_f1 = sleepeffpsg;
  else if sleepeffpsg lt 0 then nsrr_ttleffsp_f1 = .;

*nsrr_ttllatsp_f1;
*use soflopsg;
  format nsrr_ttllatsp_f1 8.2;
  if soflopsg ge 0 then nsrr_ttllatsp_f1 = soflopsg;
  else if soflopsg lt 0 then nsrr_ttllatsp_f1 = .;

*nsrr_ttlprdsp_s1sr;
*use soremfsopsg;
  format nsrr_ttlprdsp_s1sr 8.2;
  if soremfsopsg ge 0 then nsrr_ttlprdsp_s1sr = soremfsopsg;
  else if soremfsopsg lt 0 then nsrr_ttlprdsp_s1sr = .;

*nsrr_ttldurws_f1;
*use wasoqcpsg;
  format nsrr_ttldurws_f1 8.2;
  if wasoqcpsg ge 0 then nsrr_ttldurws_f1 = wasoqcpsg;
  else if wasoqcpsg lt 0 then nsrr_ttldurws_f1 = .;

*nsrr_pctdursp_s1;
*use pertsts1psg;
  format nsrr_pctdursp_s1 8.2;
  if pertsts1psg ge 0 then nsrr_pctdursp_s1 = pertsts1psg;
  else if pertsts1psg lt 0 then nsrr_pctdursp_s1 = .;
   
*nsrr_pctdursp_s2;
*use pertsts2psg;
  format nsrr_pctdursp_s2 8.2;
  if pertsts2psg ge 0 then nsrr_pctdursp_s2 = pertsts2psg;
  else if pertsts2psg lt 0 then nsrr_pctdursp_s2 = .;

*recode pertsts3psg;
  format pertsts3psg_recode 8.2;
  if pertsts3psg ge 0 then pertsts3psg_recode = pertsts3psg;
  else if pertsts3psg lt 0 then pertsts3psg_recode = .;

*recode pertsts4psg;
  format pertsts4psg_recode 8.2;
  if pertsts4psg ge 0 then pertsts4psg_recode = pertsts4psg;
  else if pertsts4psg lt 0 then pertsts4psg_recode = .;
  
*nsrr_pctdursp_s3;
*use pertsts3psg and pertsts4psg;
  format nsrr_pctdursp_s3 8.2;
  nsrr_pctdursp_s3 = pertsts3psg_recode+pertsts4psg_recode;
  
*nsrr_pctdursp_sr;
*use pertstrempsg;
  format nsrr_pctdursp_sr 8.2;  
  if pertstrempsg ge 0 then nsrr_pctdursp_sr = pertstrempsg;
  else if pertstrempsg lt 0 then nsrr_pctdursp_sr = .;
  
*nsrr_ttlprdbd_f1;
*use tibpsg;
  format nsrr_ttlprdbd_f1 8.2;
  if tibpsg ge 0 then nsrr_ttlprdbd_f1 = tibpsg;
  else if tibpsg lt 0 then nsrr_ttlprdbd_f1 = .; 
  
  keep 
    nsrrid
    visitn
    fileid
    site
    age_at_enrollment
    nsrr_age
    nsrr_age_gt89
    nsrr_sex
    nsrr_race
    nsrr_bmi
    nsrr_bp_systolic
    nsrr_bp_diastolic
    nsrr_current_smoker
    nsrr_ever_smoker
  nsrr_ahi_chicago1999
  nsrr_ttldursp_f1
  nsrr_phrnumar_f1
  nsrr_ttleffsp_f1
  nsrr_ttllatsp_f1
  nsrr_ttlprdsp_s1sr
  nsrr_ttldurws_f1
  nsrr_pctdursp_s1
  nsrr_pctdursp_s2
  nsrr_pctdursp_s3
  nsrr_pctdursp_sr
  nsrr_ttlprdbd_f1
    ;
run;

*******************************************************************************;
* checking harmonized datasets ;
*******************************************************************************;
/* Checking for extreme values for continuous variables */

proc means data=apples_harmonized;
VAR   nsrr_age
    nsrr_bmi
    nsrr_bp_systolic
    nsrr_bp_diastolic
  nsrr_ahi_chicago1999
  nsrr_ttldursp_f1
  nsrr_phrnumar_f1
  nsrr_phrnumar_f1
  nsrr_ttleffsp_f1
  nsrr_ttllatsp_f1
  nsrr_ttlprdsp_s1sr
  nsrr_ttldurws_f1
  nsrr_pctdursp_s1
  nsrr_pctdursp_s2
  nsrr_pctdursp_s3
  nsrr_pctdursp_sr
  nsrr_ttlprdbd_f1
  ;
run;

/* Checking categorical variables */

proc freq data=apples_harmonized;
table   nsrr_age_gt89
    nsrr_sex
    nsrr_race
    nsrr_current_smoker
    nsrr_ever_smoker;
run;

*******************************************************************************;
* make all variable names lowercase ;
*******************************************************************************;
  options mprint;
  %macro lowcase(dsn);
       %let dsid=%sysfunc(open(&dsn));
       %let num=%sysfunc(attrn(&dsid,nvars));
       %put &num;
       data &dsn;
             set &dsn(rename=(
          %do i = 1 %to &num;
          %let var&i=%sysfunc(varname(&dsid,&i));    /*function of varname returns the name of a SAS data set variable*/
          &&var&i=%sysfunc(lowcase(&&var&i))         /*rename all variables*/
          %end;));
          %let close=%sysfunc(close(&dsid));
    run;
  %mend lowcase;

  %lowcase(apples_nsrr);

*******************************************************************************;
* export nsrr csv datasets ;
*******************************************************************************;
  proc export data=apples_nsrr
    outfile="&releasepath\&version\apples-dataset-&version..csv"
    dbms=csv
    replace;
  run;
  
  proc export
    data=apples_harmonized
    outfile="&releasepath\&version\apples-harmonized-dataset-&version..csv"
    dbms=csv
    replace;
  run;
