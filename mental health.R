library(tidyverse)
library(readxl)
library(odbc)
library(tidygeocoder)
library(tigris)
library(sf)
library(cincy)
options(tigris_use_cache = TRUE)

match <- read_excel("Matched1024.xlsx", col_types = c(rep("text", 3), "skip")) 

school_type <- read_delim(
  "school.txt", 
  col_names = c(NA, "School", "SchoolType", NA),
  col_types = "_cc_",
  skip = 1
)

schools <- read_excel(
  "oct_hdcnt_fy23.xls",
  sheet = "fy23_hdcnt_bldg",
  col_types = c("skip", "text", "skip", "text", "skip", rep("text", 27))
) |>
  filter(DIST_NAME == "Cincinnati Public Schools") |>
  mutate(
    across(GRADE_OTHER:STUDENT_MULTI, \(x) ifelse(x == "< 10", NA, x)),
    across(GRADE_OTHER:STUDENT_MALE, as.numeric),
    Total = STUDENT_FEMALE + STUDENT_MALE,
    Hispanic = coalesce(STUDENT_LATINO, 1),
    Other = Total-Hispanic-STUDENT_BLACK
    ) |>
  rename(
    Male = STUDENT_MALE,
    Female = STUDENT_FEMALE,
    School = BLDG_NAME,
    Black = STUDENT_BLACK
    ) |>
  mutate(
    School = str_replace(School, "High School", "HS"),
    School = str_remove(School, "[.]"),
    School = str_replace(School, "Rising Stars", "RS"),
    School = case_when(
      School == "College Hill Fundamental Academy" ~ "College Hill Academy",
      School == "Fairview-Clifton German Language School" ~ 
        "Fairview Clifton School",
      School == "George Hays-Jennie Porter School" ~ "Hays Porter School",
      School == "Spencer Center for Gifted and Exceptional Students" ~ 
        "Spencer Center",
      School == "Leap Academy at North Fairmount" ~ "LEAP Academy",
      School == "Clifton Area Neighborhood School" ~ "CANS School",                  
      School == "James N Gamble Montessori Elementary School" ~ 
        "Gamble Montessori Elementary",
      School == "RS at Aiken New Tech/College Hill" ~ 
        "Rising Stars Academy Aiken New Tech",
      School == "RS at Cheviot/Westwood" ~ "RS at Cheviot Westwood",
      School == "North Avondale Montessori School" ~ "North Avondale Montessori",
      School == "Pleasant Ridge Montessori School" ~ "Pleasant Ridge Montessori",
      School == "William H Taft Elementary School" ~ "Taft Elementary",
      School == "Robert A Taft Information Technology HS" ~ "Robert A Taft IT HS",
      School == "Parker Woods Montessori School" ~ "Parker Woods Montessori",
      School == "School For Creative and Performing Arts" ~ "SCPA",
      School == "Academy Of World Languages School" ~ "AWL School",
      School == "Academy for Multilingual Immersion Studies" ~ "AMIS School",
      School == "Aiken New Tech HS" ~ "Aiken HS",
      TRUE ~ School
      )
    ) |>
  left_join(school_type) |>
  mutate(
    PctMale = Male/Total,
    Black_Male = Black*PctMale,
    Black_Female = Black-Black_Male,
    Other_Male = Other*PctMale,
    Other_Female = Other-Other_Male,
    Hispanic_Male = Hispanic*PctMale,
    Hispanic_Female = Hispanic-Hispanic_Male) |>
  select(School, Black_Male:Hispanic_Female) |>
  pivot_longer(Black_Male:Hispanic_Female) |>
  separate_wider_delim(name, delim = "_", names = c("Race", "Gender")) |>
  mutate(Students = ceiling(value)) |>
  select(-value)

students <- as.data.frame(lapply(schools, rep, schools$Students)) |>
  group_by(School, Race, Gender) |>
  mutate(StudentID = row_number()) |>
  select(-Students)

con <- dbConnect(odbc::odbc(), "ClarityProd")

admits <- dbGetQuery(con, "
  SELECT DISTINCT r.pat_enc_csn_id
                  ,r.pat_id
                  ,p.gender as Gender
                  ,rem.mapped_ethnic_group_c as Ethnicity
                  ,rem.mapped_race as Race
                  ,CAST(r.birth_date AS DATE) AS birth_date
                  ,CAST(r.hosp_admsn_time AS DATE) AS contact_date
                  ,p.add_line_1
                  ,p.add_line_2
                  ,p.city
                  ,p.state
                  ,p.zip
                  ,r.disch_icd_1
                  ,r.disch_dx_name_1
		              ,ccs.default_ccsr_category_description_ip as category
		              ,1 as Admission
		              ,0 as ED
  FROM hpceclarity.bmi.readmissions r
		INNER JOIN hpceclarity.bmi.patient p
		  ON r.pat_id = p.pat_id
		INNER JOIN hpceclarity.bmi.y_chmc_race_ethnicity_mapping rem
		  ON r.pat_id = rem.pat_id
		LEFT JOIN temptable.dbo.icd10_ccs_2021 ccs
		  ON r.disch_icd_1 = ccs.icd_10_cm_code
  WHERE r.hosp_admsn_time >= '2019-01-01' 
    AND (r.disch_icd_1 like 'F%'
		    OR r.disch_icd_1 like 'R45%'
		    OR (r.department_name in ('LCH LCOH'))
		    OR (r.department_name in ('A4C2') 
		      AND r.hosp_admsn_time < '2018-03-01')
		    OR (r.department_name in ('P2W', 'P3E', 'P3N', 'P3S', 'P3SW') 
		      AND r.hosp_admsn_time < '2020-05-10')
		    OR (r.department_name in ('ZZPB2-200') 
		      AND r.hosp_admsn_time BETWEEN '2015-09-01 00:00' and '2020-05-10')
		    OR (r.department_name in ('P2E') 
		      AND r.hosp_admsn_time BETWEEN '2016-04-01 00:00' and '2020-05-10')
		    OR (r.department_name in ('P2N') 
		      AND r.hosp_admsn_time BETWEEN '2018-02-01 00:00' and '2020-05-10')
		    OR (r.department_name in ('P2SW') 
		      AND r.hosp_admsn_time BETWEEN '2018-08-01 00:00' and '2020-05-10')
		    OR (r.department_name in ('PA2E', 'PA2N', 'PA2SW', 'PA2W', 'PA3E', 
		                              'PA3N', 'PA3S', 'PA3SW', 'PA3W', 'PB2-200') 
		      AND r.hosp_admsn_time > '2020-05-10')
    )
                     ")

ed <- dbGetQuery(con, "
  SELECT p.pat_id
		,ed.enc_id as pat_enc_csn_id
		,ed.dispo
		,CAST(ed.arrvdate AS DATE) AS contact_date
		,CAST(ed.dschdate AS DATE) AS dschdate
		,ed.diagnosis1 as disch_dx_name_1
		,ed.current_icd10_list as disch_icd_1
		,p.gender as Gender
		,p.add_line_1
		,p.add_line_2
    ,p.city
		,p.state
		,p.zip
		,p.birth_date
		,rem.mapped_ethnic_group_c as Ethnicity
    ,rem.mapped_race as Race
    ,ccs.default_ccsr_category_description_ip as category
    ,0 as Admission
    ,1 as ED
  FROM hpceclarity.dbo.chmc_ed_daily_jcaho ed
    INNER JOIN hpceclarity.bmi.patient p
      ON ed.mrn = p.pat_mrn_id
	  INNER JOIN hpceclarity.bmi.y_chmc_race_ethnicity_mapping rem
	  	ON p.pat_id = rem.pat_id
	  LEFT JOIN temptable.dbo.icd10_ccs_2021 ccs
		  ON ed.current_icd10_list = ccs.icd_10_cm_code
	WHERE arrvdate >= '1/1/2019'
		AND dispo not like '%LWBS%'
		AND dispo <> 'ED Dismiss - Never Arrived'
		AND site not like '%URGENT%'
		AND room_name not like '%URG%'
		AND room_name not like '%MAS%'
		AND room_name not like '%UC%'
		AND room_name not like '%AND%'
		AND room_name not like '%GRN%'
		AND (current_icd10_list like 'F%'
			OR current_icd10_list like 'R45%')
                 ") 

admit_match <- inner_join(admits, match)
ed_match <- inner_join(ed, match)

adds <- select(admit_match, add_line_1, city, state, zip) |>
  rbind(select(ed_match, add_line_1, city, state, zip)) |>
  unique() |>
  filter(str_starts(add_line_1, "222 ", negate = TRUE)) |>
  mutate(AddID1 = row_number())

admit_add <- left_join(admit_match, adds)
ed_add <- left_join(ed_match, adds)

adds <- adds |>
  mutate(
    City = str_to_title(city),
    Zip = str_trunc(zip, 5, "right", ellipsis = ""),
    Address = str_to_title(add_line_1)
    ) |>
  separate_wider_delim(
    Address, delim = "Apt ", names = "Address", too_many = "drop"
  ) |>
  separate_wider_delim(
    Address, delim = "#", names = "Address", too_many = "drop"
  ) |>
  separate_wider_delim(
    Address, delim = "Unit ", names = "Address", too_many = "drop"
  ) |>
  separate_wider_delim(
    Address, delim = "Fl ", names = "Address", too_many = "drop"
  ) 
  
to_geocode <- adds |>
  distinct(Address, City, state, Zip) |>
  mutate(AddID2 = row_number())

adds <- left_join(adds, to_geocode)
admit_add <- left_join(admit_add, adds)
ed_add <- left_join(ed_add, adds)

geocoded <- geocode(
  to_geocode,
  street = Address,
  city = City,
  state = state,
  postalcode = Zip,
  method = "census"
) 

uncoded <- filter(geocoded, is.na(lat)) |>
  select(Address:AddID2) |>
  geocode(
    street = Address,
    city = City,
    state = state,
    postalcode = "Zip"
  )

coded <- filter(geocoded, !is.na(lat)) |>
  rbind(filter(uncoded, !is.na(lat))) |>
  st_as_sf(coords = c("long", "lat"), crs = 'NAD83', remove = FALSE)

uncoded2 <- filter(uncoded, is.na(lat)) |>
  select(-c(lat, long)) |>
  mutate(
    AddressNew = str_replace(Address, "Highline", "High Line"),
    AddressNew = str_replace(AddressNew, "Hoolster", "Hollister"),
    AddressNew = str_replace(AddressNew, "Western", "Westwood"),
    AddressNew = str_replace(AddressNew, "Woodlen", "Woodland"),
    AddressNew = str_replace(AddressNew, "Wymong", "Wyoming"),
    AddressNew = str_replace(AddressNew, "Glen St", "Glen Este"),
    AddressNew = str_replace(AddressNew, "Werd", "Werk"),
    AddressNew = str_replace(AddressNew, "Global", "Gobel")
  ) |>
  geocode(
    street = AddressNew,
    city = City,
    state = state,
    postalcode = Zip
  )

coded <- filter(uncoded2, !is.na(lat)) |>
  select(-AddressNew) |>
  st_as_sf(coords = c("long", "lat"), crs = 'NAD83', remove = FALSE) |>
  rbind(coded)

hooded1 <- filter(uncoded2, is.na(lat)) |>
  mutate(
    Neighborhood = case_when(
      str_detect(Address, "Lilly") ~ "Bond Hill",
      str_detect(Address, "Martin") ~ "Cheviot",
      str_detect(Address, "Berchwood") ~ "Out of district",
      TRUE ~ "Unknown"
    )
  ) |>
  select(AddID2, Neighborhood)

muni_lines <- county_subdivisions(
  state = "OH",
  county = "Hamilton",
  year = 2021
) |>
  mutate(
    Municipality = str_remove(NAMELSAD, " city"),
    Municipality = str_remove(Municipality, " village"),
    Municipality = str_remove(Municipality, "The Village of "),
    Municipality = str_to_title(Municipality)
  ) |>
  select(Municipality, geometry)

munis <- st_join(coded, muni_lines, left = FALSE)
hooded2 <- anti_join(coded, as_tibble(munis)) |>
  as_tibble() |>
  select(AddID2) |>
  mutate(Neighborhood = "Unknown") 

hooded3 <- filter(munis, Municipality != "Cincinnati") |>
  rename(Neighborhood = Municipality) |>
  as_tibble() |>
  select(AddID2, Neighborhood)

cinci <- filter(munis, Municipality == "Cincinnati")

hooded4 <- neigh_sna |> 
  mutate(
    SHAPE = st_transform(SHAPE, crs = "NAD83"),
    Neighborhood = case_when(
      neighborhood == "North Avondale - Paddock Hills" ~ 
        "North Avondale-Paddock Hills",
      neighborhood %in% c("Lower Price Hill", "Queensgate") ~ 
        "Lower Price Hill-Queensgate",
      neighborhood == "Villages at Roll Hill" ~ "Roll Hill",
      TRUE ~ neighborhood
      )
    ) |>
  group_by(Neighborhood) |>
  summarise(geometry = st_union(SHAPE)) |>
  st_join(cinci, left = FALSE) |>
  as_tibble() |>
  select(AddID2, Neighborhood)

hooded <- rbind(hooded1, hooded2) |>
  rbind(hooded3) |>
  rbind(hooded4) |>
  right_join(adds, multiple = "all") |>
  select(AddID1, Neighborhood) 

admit_final <- left_join(admit_add, hooded) |>
  mutate(Neighborhood = coalesce(Neighborhood, "Unknown")) |>
  select(pat_enc_csn_id:contact_date, category:ED, School, Neighborhood)

ed_final <- left_join(ed_add, hooded) |>
  mutate(Neighborhood = coalesce(Neighborhood, "Unknown")) |>
  select(
    pat_id, 
    pat_enc_csn_id, 
    contact_date, 
    Gender, 
    birth_date:ED, 
    School, 
    Neighborhood
    )

hospital <- rbind(admit_final, ed_final) |>
  group_by(pat_enc_csn_id) |>
  mutate(ED = max(ED)) |>
  filter(Admission == max(Admission)) |>
  ungroup() |>
  mutate(
    Race = case_when(
      Ethnicity == "Hispanic" ~ Ethnicity,
      Race == "Black or African American" ~ "Black",
      TRUE ~ "Other"
      )
    ) |>
  select(-Ethnicity)
  
studentid <- hospital |>
  ungroup() |>
  distinct(pat_id, School, Race, Gender) |>
  group_by(School, Race, Gender) |>
  mutate(StudentID = row_number()) |>
  inner_join(hospital, multiple = "all")


full_set <- left_join(students, studentid, multiple = "all") |>
  ungroup() |>
  mutate(
    RN = as.character(row_number()),
    PersonalID = coalesce(pat_id, RN)
  ) |>
  select(-c(RN, StudentID, pat_id, birth_date)) |>
  inner_join(school_type) |>
  mutate(
    School = str_remove(School, " School"),
    School = str_remove(School, " HS"),
    PatientID = ifelse(Admission == 1 | ED == 1, PersonalID, NA)
    )

#write_csv(full_set, "for_pbi6.csv")

grade <- read_excel(
  "oct_hdcnt_fy23.xls",
  sheet = "fy23_hdcnt_bldg",
  col_types = c(
    "skip", "text", "skip", "text", "skip", rep("numeric", 13), rep("skip", 14)),
  col_names = c(
    NA, 
    "District", 
    NA, 
    "School", 
    NA, 
    paste0("Grade", c("K", 1:12)), 
    rep(NA, 14)
    ),
  skip = 1
  ) |>
  filter(District == "Cincinnati Public Schools") |>
  mutate(across(GradeK:Grade12, \(x) coalesce(x, 0))) |>
  summarise(across(GradeK:Grade12, sum)) |>
  pivot_longer(
    GradeK:Grade12,
    names_to = "Grade",
    values_to = "Students"
    ) |>
  mutate(Grade = str_remove(Grade, "Grade"))

grade_admit <- hospital |>
  mutate(
    SchoolYear = case_when(
      contact_date >= "2023-09-01" ~ 2023,
      contact_date >= "2022-09-01" ~ 2022,
      contact_date >= "2021-09-01" ~ 2021,
      contact_date >= "2020-09-01" ~ 2020,
      contact_date >= "2019-09-01" ~ 2019,
      TRUE ~ 2018
    ),
    SchoolAge = floor(
      as.numeric(as.Date(paste(SchoolYear, "09", "01", sep = "-"))-birth_date)/
        365.25
      ),
    Grade = ifelse(SchoolAge < 6, "K", SchoolAge-5)
  ) |>
  group_by(Grade) |>
  summarise(
    Patients = length(unique(pat_id)),
    Admissions = sum(Admission),
    ED = sum(ED)
  ) |>
  full_join(grade) |>
  mutate(
    PatientRate = 1000*Patients/Students,
    AdmissionRate = 1000*Admissions/Students,
    GradeNo = ifelse(Grade == "K", 1, as.numeric(Grade)+1)
    ) |>
  filter(GradeNo < 14)
write_csv(grade_admit, "grade_for_pbi.csv")
