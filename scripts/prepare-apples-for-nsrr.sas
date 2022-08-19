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
  libname applesi "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20220301-kushida-apples\nsrr-prep\_ids";

  *set data dictionary version;
  %let version = 0.1.0.pre;

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

  *merge datasets;
  data apples_in;
    length appleid $7 visitn 8. visit $4;
    merge
      apples_bl_char_in
      apples_wasi_in
      apples_bsrt_in
      apples_cpap_in
      apples_ess_in
      apples_poms_in
      apples_psg_in
      apples_pvt_in
      ;
    by appleid visit;

    *'empty' visit rows in BSRT and PVT datasets - remove these empty rows entirely;
    if visit = '' then delete;

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
      ;
  run;

  proc sort data=apples_in;
    by appleid visitn;
  run;

  data apples_retain;
    set apples_in;
    by appleid;

    retain gender2 ethnicity2;
    if first.appleid then do;
      gender2 = gender;
      ethnicity2 = ethnicity;
    end;
  run;

  data apples_nsrr;
    set apples_retain;

    gender = gender2;
    ethnicity = ethnicity2;

    drop
      gender2
      ethnicity2
      ;
  run;

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
  set apples_nsrr;

*demographics
*age;
*use age;
  format nsrr_age 8.2;
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
  if gender = '0)' then nsrr_sex = 'male';
  else if gender = '1)' then nsrr_sex = 'female';
  else if gender = '.' then nsrr_sex = 'not reported';

*race;
*use gierace;
    format nsrr_race $100.;
    if gierace = 1 then nsrr_race = 'white';
    else if gierace = 2 then nsrr_race = 'black or african american';
    else if gierace = 3 then nsrr_race = 'asian';
    else if gierace = 4 then nsrr_race = 'hispanic';
  else if gierace = 5 then nsrr_race = 'other';
  else  nsrr_race = 'not reported';

*ethnicity;
*use gierace;
  format nsrr_ethnicity $100.;
    if gierace = 4 then nsrr_ethnicity = 'hispanic or latino';
    else if gierace = 1 then nsrr_ethnicity = 'not hispanic or latino';
  else if gierace = 2  then nsrr_ethnicity = 'not hispanic or latino';
  else if gierace = 3   then nsrr_ethnicity = 'not hispanic or latino';
  else if gierace = 5  then nsrr_ethnicity = 'not hispanic or latino';
  else if gierace = '.' then nsrr_ethnicity = 'not reported';

*anthropometry
*bmi;
*use hwbmi;
  format nsrr_bmi 10.9;
  nsrr_bmi = hwbmi;

*clinical data/vital signs
*bp_systolic;
*use bpbpsysm;
  format nsrr_bp_systolic 8.2;
  nsrr_bp_systolic = bpbpsysm;

*bp_diastolic;
*use bpbpdiam;
  format nsrr_bp_diastolic 8.2;
  nsrr_bp_diastolic = bpbpdiam;

*lifestyle and behavioral health
*current_smoker;
*use tusmknow;
  format nsrr_current_smoker $100.;
    if tusmknow = '1' then nsrr_current_smoker = 'yes';
    else if tusmknow = '0' then nsrr_current_smoker = 'no';
    else if tusmknow = 'A'  then nsrr_current_smoker = 'not reported';
    else if tusmknow = 'D'  then nsrr_current_smoker = 'not reported';
  else if tusmknow = 'K'  then nsrr_current_smoker = 'not reported';
  else if tusmknow = 'M'  then nsrr_current_smoker = 'not reported';

*ever_smoker;
*use tursmoke;
  format nsrr_ever_smoker $100.;
    if tursmoke = '1' then nsrr_ever_smoker = 'yes';
    else if tursmoke = '2' then nsrr_ever_smoker = 'yes';
    else if tursmoke = '0'  then nsrr_ever_smoker = 'no';
    else if tursmoke = 'A'  then nsrr_ever_smoker = 'not reported';
  else nsrr_ever_smoker = 'not reported';


*******************************************************************************;
* checking harmonized datasets ;
*******************************************************************************;

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
