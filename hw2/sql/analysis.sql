-- 1. Текущий спрос по регионам и профессиональным сферам.
select
    snapshot_date,
    region_name,
    sphere_name,
    vacancy_count,
    work_place_count
from published.mart_regional_sphere_demand
order by snapshot_date desc, vacancy_count desc;

-- 2. Сферы с самой высокой средней предлагаемой зарплатой.
select
    region_name,
    sphere_name,
    vacancy_count,
    average_salary_min,
    average_salary_max
from published.mart_regional_sphere_demand
where vacancy_count >= 10
order by average_salary_max desc nulls last
limit 30;

-- 3. Где чаще готовы брать без опыта.
select
    region_name,
    sphere_name,
    vacancy_count,
    no_experience_share
from published.mart_regional_sphere_demand
where vacancy_count >= 10
order by no_experience_share desc, vacancy_count desc
limit 30;

-- 4. Проверка ключа снимка: результат должен быть равен нулю.
select count(*) - count(distinct (snapshot_date, vacancy_id)) as duplicates
from analytics_candidate.fct_vacancies;
