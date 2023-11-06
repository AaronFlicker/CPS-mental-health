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
      School == "Fairview-Clifton German Language School" ~ "Fairview Clifton School",
      School == "George Hays-Jennie Porter School" ~ "Hays Porter School",
      School == "Spencer Center for Gifted and Exceptional Students" ~ "Spencer Center",
      School == "Leap Academy at North Fairmount" ~ "LEAP Academy",
      School == "Clifton Area Neighborhood School" ~ "CANS School",                  
      School == "James N Gamble Montessori Elementary School" ~ "Gamble Montessori Elementary",
      School == "RS at Aiken New Tech/College Hill" ~ "Rising Stars Academy Aiken New Tech",
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
                     ") |>
  mutate(
    dispo = NA,
    AdmitID = pat_enc_csn_id,
    EDID = NA
    )

ed <- dbGetQuery(con, "
  SELECT p.pat_id
		,ed.enc_id as pat_enc_csn_id
		,ed.dispo
		,CAST(ed.arrvdate AS DATE) AS contact_date
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
                 ") |>
  mutate(
    AdmitID = NA,
    EDID = pat_enc_csn_id
  )

admit_ed <- rbind(admits, ed)

matched <- inner_join(match, admit_ed, multiple = "all") |>
  mutate(
    Race = case_when(
      Ethnicity == "Hispanic" ~ Ethnicity,
      str_detect(Race, "Black") ~ "Black",
      TRUE ~ "Other"
    )
  ) |>
  inner_join(school_type)

adds <- matched |>
  distinct(add_line_1, city, state, zip) |>
  ungroup() |>
  filter(str_starts(add_line_1, "222 ", negate = TRUE)) |>
  mutate(AddID1 = row_number())

matched <- left_join(matched, adds) 

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
      str_detect(Address, "Berchwood") ~ "Colerain Township",
      TRUE ~ "Out of district"
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

hooded2 <- filter(munis, Municipality != "Cincinnati") |>
  rename(Neighborhood = Municipality) |>
  as_tibble() |>
  select(AddID2, Neighborhood)

cinci <- filter(munis, Municipality == "Cincinnati")

hooded3 <- neigh_sna |> 
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
  right_join(adds, multiple = "all") |>
  mutate(Neighborhood = coalesce(Neighborhood, "Out of district")) |>
  select(AddID1, Neighborhood)

matched2 <- ungroup(matched) |>
  left_join(hooded) |>
  mutate(
    Neighborhood = case_when(
      is.na(add_line_1) ~ "Out of district",
      is.na(Neighborhood) ~ "Foster care",
      TRUE ~ Neighborhood
      )
    )

studentid <- matched2 |>
  distinct(pat_id, School, Race, Gender) |>
  group_by(School, Race, Gender) |>
  mutate(StudentID = row_number()) |>
  inner_join(matched2, multiple = "all") |>
  select(pat_id:StudentID, contact_date, category, AdmitID, EDID, Neighborhood)

full_set <- left_join(students, studentid, multiple = "all") |>
  ungroup() |>
  mutate(
    RN = as.character(row_number()),
    PersonalID = coalesce(pat_id, RN)
  ) |>
  select(-c(RN, StudentID, pat_id)) |>
  inner_join(school_type) |>
  mutate(
    School = str_remove(School, " School"),
    School = str_remove(School, " HS"),
    Neighborhood = str_replace(Neighborhood, "North ", "N. "),
    Neighborhood = str_replace(Neighborhood, "South ", "S. "),
    Neighborhood = str_replace(Neighborhood, "East ", "E. "),
    #Neighborhood = str_replace(Neighborhood, "West ", "W. "),
    Neighborhood = str_replace(Neighborhood, "Township", "Twp."),
    Admissions = ifelse(is.na(AdmitID), 0, 1),
    EDVisits = ifelse(is.na(EDID), 0, 1),
    PatientID = ifelse(Admissions == 1 | EDVisits == 1, PersonalID, NA)
    )

#write_csv(full_set, "for_pbi6.csv")

grade <- read_excel(
  "oct_hdcnt_fy23.xls",
  sheet = "fy23_hdcnt_bldg",
  col_types = c(
    "skip", "text", "skip", "text", "skip", rep("numeric", 13), rep("skip", 14)),
  col_names = c(NA, "District", NA, "School", NA, paste0("Grade", c("K", 1:12)), rep(NA, 14)),
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

grade_admit <- matched2 |>
  mutate(
    SchoolYear = case_when(
      contact_date >= "2023-09-01" ~ 2023,
      contact_date >= "2022-09-01" ~ 2022,
      contact_date >= "2021-09-01" ~ 2021,
      contact_date >= "2020-09-01" ~ 2020,
      contact_date >= "2019-09-01" ~ 2019,
      TRUE ~ 2018
    ),
    SchoolAge = floor(as.numeric(as.Date(paste(SchoolYear, "09", "01", sep = "-"))-birth_date)/365.25),
    Grade = ifelse(SchoolAge < 6, "K", SchoolAge-5)
  ) |>
  group_by(Grade) |>
  summarise(
    Patients = length(unique(pat_id)),
    Admissions = n()
  ) |>
  full_join(grade) |>
  mutate(
    PatientRate = 1000*Patients/Students,
    AdmissionRate = 1000*Admissions/Students,
    GradeNo = ifelse(Grade == "K", 1, as.numeric(Grade)+1)
    )
write_csv(grade_admit, "grade_for_pbi.csv")
