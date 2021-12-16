# United States Immigration Data Analysis
## Data Dictionary
---
### fact_immigration_i94

| Column | Description | Type|
| ----------- | ----------- | ----------- |
| cic_id | I.D. for immigration record | string|
| i94_year | 4 digit year of I94 record | string|
| i94_month | Numeric month of I94 record | string|
| i94_cit & i94_res  | Origin/destination country | string|
| i94_city_cd | 3 character port code that is used for city | string|
| arr_date | Date of arrival | string|
| dep_date | Date of departure | string|
| i94_state_cd | 2 character state code | string|
| i94_resp_age | Age of respondent | string|
| i94_visa_cd | Visa type code | string|
| visa_post | Department of State where Visa was issues | string|
| us_adm_date | Date to which admitted to U.S. | string|
| adm_num | Admission number | string|
| gender | Non-immigrant sex | string|
| ins_num | INS number | string|
| airline | Airline used to arrive to the U.S. | string|
| fl_num | Flight number of Airline used to arrive | string|

---
### dim_country
| Column | Description | Type|
| ----------- | ----------- | ----------- |
| country_cd | Code that is used to reference a country | string|
| country_name | Corresponding name of the country | string|

---
### dim_visa
| Column | Description | Type|
| ----------- | ----------- | ----------- |
| visa_cd | Visa type cd | string|
| visa_descr | Corresponding description of the visa | string|

---
### dim_city
| Column | Description | Type|
| ----------- | ----------- | ----------- |
| city_cd | 3 character port code that is used for city | string|
| city_name | Corresponding name of the city | string|


---
### dim_state
| Column | Description | Type|
| ----------- | ----------- | ----------- |
| state_cd | 2 character state code | string|
| state_name | Corresponding name of the state | string|


---
### dim_city_demographics
| Column | Description | Type|
| ----------- | ----------- | ----------- |
| city | City name | string|
| state_cd | 2 character state code | string|
| median_age | Median age in the city | string|
| male_pop | Count of male population in the city | string|
| female_pop | Count of female population in the city | string|
| total_pop | Count of population in the city | string|
| foreign_born_pop | Count of foreign born population in the city | string|
| avg_hh_size | Average household size in the city | string|
| avg_temp | Most recent average temperature from the dataset, 2013, in Fahrenheit | double|


