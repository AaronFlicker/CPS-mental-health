library(tidyverse)
library(readxl)
library(odbc)
library(tidygeocoder)
library(tigris)
library(sf)
options(tigris_use_cache = TRUE)

match <- read_excel("Matched.xlsx", col_types = c(rep("text", 3), "skip")) |>
  unique() |>
  group_by(pat_id) |>
  mutate(count = n()) |>
  arrange(-count, pat_id)

student <- read_csv(
  "student with neighborhood.csv",
  col_types = "__c_c_cc_____c_______________________c",
  col_names = c(
    NA, 
    NA, 
    "StudentID", 
    NA, 
    "Gender", 
    NA, 
    "Ethnicity", 
    "Race", 
    rep(NA, 5), 
    "SchoolID", 
    rep(NA, 23), 
    "Neighborhood"
    ),
  skip = 1
  )

allocations <- read.csv(
  "neighborhood bg allocations.csv",
  colClasses = c(
    "character", 
    rep("NULL", 3), 
    "character", 
    "numeric", 
    "character"
  )
)

school <- read_delim(
  "school.txt", 
  col_names = c("SchoolID", "School", "SchoolType", NA),
  col_types = "ccc_",
  skip = 1
  )

enroll <- left_join(student, school) |>
  mutate(
    Gender = ifelse(Gender == "F", "Female", "Male"),
    Ethnicity = case_when(
      Ethnicity == "Y" ~ "Hispanic",
      Ethnicity == "N" ~ "Non-Hispanic",
      TRUE ~ "Unknown"
    ),
    Race = ifelse(Race == "B", "Black", "Non-Black")
  ) |>
  group_by(Gender, Ethnicity, Race, SchoolType, School, Neighborhood) |>
  summarise(Enrollment = n())

con <- dbConnect(odbc::odbc(), "ClarityProd")

admits <- dbGetQuery(con, "
  SELECT DISTINCT r.pat_enc_csn_id
                  ,r.pat_id
                  ,p.gender as Gender
                  ,rem.mapped_ethnic_group_c as Ethnicity
                  ,rem.mapped_race as Race
                  ,CAST(r.birth_date AS DATE) AS birth_date
                  ,CAST(r.hosp_admsn_time AS DATE) AS contact_date
                  ,a.addr_hx_line1 as add_line_1
                  ,a.addr_hx_line2 as add_line_2
                  ,a.city_hx as city
                  ,a.state
                  ,a.zip_hx as zip
                  ,a.county
                  ,r.disch_icd_1
		              ,r.disch_icd_2
		              ,r.adt_pat_class
		              ,department_id
		              ,department_name
  FROM hpceclarity.bmi.readmissions r
		INNER JOIN hpceclarity.dbo.chmc_adt_addr_hx a
		  ON r.pat_id = a.pat_id
		INNER JOIN hpceclarity.bmi.patient p
		  ON r.pat_id = p.pat_id
		INNER JOIN hpceclarity.bmi.y_chmc_race_ethnicity_mapping rem
		  ON r.pat_id = rem.pat_id
  WHERE r.hosp_admsn_time >= a.eff_start_date
		AND (a.eff_end_date IS NULL OR r.hosp_admsn_time < a.eff_end_date)
		AND r.hosp_admsn_time >= '2018-08-01' 
		AND a.state = 'Ohio'
		AND department_id IN (20026290, 10401033, 10401059, 10401034, 10401035, 
		                      10401039, 10401036, 10401074, 10401030, 10401031, 
		                      10401081, 10401085, 10401086, 10401087, 10401088, 
		                      10401089, 10401090, 10401091, 10401092, 10401093, 
		                      10401094)
		AND a.addr_hx_line1 NOT LIKE '222 E%'
                     ") |>
  filter(department_id %in% c(10401085,	
                              10401086,	
                              10401087,	
                              10401088,	
                              10401089,	
                              10401090,	
                              10401091,	
                              10401092,	
                              10401093,	
                              10401094) | contact_date <= "2020-05-31"
         )

matched <- inner_join(match, admits, multiple = "all")

adds <- matched |>
  distinct(add_line_1, city, state, zip) |>
  ungroup() |>
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
    AddressNew = str_replace(Address, "Kenny", "Kinney"),
    AddressNew = str_replace(AddressNew, "Hoolster", "Hollister"),
    AddressNew = str_replace(AddressNew, "Wain St", "Wayne Ave"),
    AddressNew = str_replace(AddressNew, "Martin", "Martins"),
    AddressNew = str_replace(AddressNew, "Montsort Hl", "Monfort Hills Ave"),
    AddressNew = str_replace(AddressNew, "Wn", "W North"),
    AddressNew = str_replace(AddressNew, "Woodlen", "Woodland")
  ) |>
  filter(!Zip %in% c("41071", "45103")) |>
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
      str_detect(Address, "Ashtabula") ~ "Sayler Park",
      str_detect(Address, "Wain") ~ "Lockland",
      str_detect(Address, "Martin") ~ "Cheviot",
      TRUE ~ "Colerain Township"
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

bg_lines <- block_groups(
  state = "OH",
  county = "Hamilton",
  year = 2021
) |>
  inner_join(allocations, multiple = "all") |>
  filter(Municipality == "Cincinnati") |>
  group_by(Neighborhood) |>
  summarise(geometry = st_union(geometry))

hooded3 <- st_join(cinci, bg_lines) |>
  group_by(AddID2) |>
  mutate(count = n()) |>
  filter(count == 1 | Neighborhood == "College Hill") |>
  as_tibble() |>
  select(AddID2, Neighborhood)

hooded <- rbind(hooded1, hooded2) |>
  rbind(hooded3) |>
  right_join(adds, multiple = "all") |>
  mutate(Neighborhood = coalesce(Neighborhood, "Out of county")) |>
  select(AddID1, Neighborhood)

matched2 <- inner_join(matched, hooded, multiple = "all") |>
  mutate(
    Race = ifelse(Race == "Black or African American", "Black", "Non-Black")
    ) |>
  group_by(Gender, Ethnicity, Race, School, Neighborhood) |>
  summarise(
    Admissions = n(),
    Patients = length(unique(pat_id))
    )

totals <- left_join(enroll, matched2) |>
  ungroup() |>
  mutate(across(Admissions:Patients, \(x) coalesce(x, 0))) 

#write_csv(matched, "for_pbi.csv")
write_csv(totals, "for_pbi2.csv")

test <- totals |>
  group_by(Gender) |>
  summarise(Admissions = sum(Admissions)*1000, Enrollment = sum(Enrollment)) |>
  mutate(Rate = Admissions/Enrollment)
