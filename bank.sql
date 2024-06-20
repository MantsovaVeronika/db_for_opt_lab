------------------------------------------------------Задание №1------------------------------------------------------
-- Найти сотрудников, которые выезжали на объект без оборудования. 
-- Сравнить планы выполнения запроса с использованием IN и EXISTS.
------------------------------------------------------Задание №2------------------------------------------------------
-- Найти сотрудников, у которых не было опыта работы до прихода в компанию и которые были главными садовниками на выездах
analyze;
explain analyze
select e.emp_name  
from employee_on_departure emp_d 
JOIN employee e ON e.emp_id = emp_d.emp_id 
where (extract(year from current_date) - extract(year from e.begin_date) = e.work_exp) and emp_d.role_on_dep = 'Главный садовник'
group by e.emp_name;

-- вместо group by distinct, чтобы просто избавиться от дубликатов
-- в CASE тяжелое соединение
explain analyze
select e.emp_name  
FROM employee e 
where 
 	case
 		when (extract(year from current_date) - extract(year from e.begin_date) = e.work_exp) then -- не было опыта работы
 		exists(select null
 		from employee_on_departure emp_d
 		where e.emp_id = emp_d.emp_id and emp_d.role_on_dep = 'Главный садовник'
 		)
 	end;
 
 -- создание частичного индекса
create index on employee_on_departure(role_on_dep) where role_on_dep = 'Главный садовник';

explain analyze
select e.emp_name  
from employee_on_departure emp_d 
JOIN employee e ON e.emp_id = emp_d.emp_id 
where (extract(year from current_date) - extract(year from e.begin_date) = e.work_exp) and emp_d.role_on_dep = 'Главный садовник'
group by e.emp_name;

-- удаление индекса
 drop index employee_on_departure_role_on_dep_idx;
------------------------------------------------------Задание №3------------------------------------------------------
-- Представить отчет об обороте оборудования: для каждого оборудования, у которого не истек срок эксплуатации,
-- вывести информацию о том, сколько раз его брали на объекты, среднее количество этого оборудования на одном
-- выезде и список сотрудников, бравших его. Дан запрос, требующий оптимизации.

explain analyze
select  eq.eq_id, 
		(select avg(eod.eq_count)
			from equipment_on_departure eod
			where eq.eq_id = eod.eq_id
		) as avg_eq,
		(select count(*) 
			from equipment_on_departure eod
			where eq.eq_id = eod.eq_id
			and eod.return_eq = 'R'
		) as cnt_take,
		(select string_agg(distinct emp.emp_name, '; ') 
			from equipment_on_departure eod
			join employee emp
				on emp.emp_id = eod.emp_id 
			where eq.eq_id = eod.eq_id
			and eod.return_eq = 'R'
		) as emp_list
from equipment eq
where eq.do_date::int + eq.eq_life::int >= 2024;

-- сокращение числа подзапросов
explain analyze
select  eq.eq_id,
		avg_eq, 
		cnt_take,
		emp_list
	from equipment eq
	left join lateral (select avg(eod.eq_count) as avg_eq,
							  count(*) filter(where eod.return_eq = 'R') as cnt_take,
							  string_agg(distinct emp.emp_name, '; ') filter(where eod.return_eq = 'R') as emp_list
						from equipment_on_departure eod 
						left join employee emp
							 on emp.emp_id = eod.emp_id 
						where eq.eq_id = eod.eq_id
							) eod
		on (1 = 1)	
	where eq.do_date::int + eq.eq_life::int >= 2024;

-- создание индекса
create index on equipment_on_departure(eq_id);

-- удаление индекса
drop index equipment_on_departure_eq_id;
------------------------------------------------------Задание №4------------------------------------------------------
-- Найти выезды, сумма по которым является максимальной из всех выездов, выполненных на тот же объект
analyze;
explain analyze
select *
from departure d1 
where d1.dep_sum = (
select max(d2.dep_sum)
from departure d2
where d1.fac_id = d2.fac_id
); 

--create index on departure(fac_id);
create index on departure(dep_sum); -- Index Scan
create index on departure(dep_sum, fac_id); -- Index Only Scan

-- индексы не нужны
explain analyze
select *
from departure d1, (
select max(d2.dep_sum) maxsum, d2.fac_id
from departure d2
group by d2.fac_id) d3
where d1.fac_id = d3.fac_id and d1.dep_sum = d3.maxsum;


-- удаление всех индексов
--drop index departure_fac_id_idx;
drop index departure_dep_sum_idx;
drop index departure_dep_sum_fac_id_idx;
------------------------------------------------------Задание №5------------------------------------------------------
-- Найти 10 единиц оборудования за последнее время, которое либо взяли и не вернули на склад, либо взяли
-- в колчестве больше 1, причем его стоимость больше в 5 раз от средне стоимости всего оборудования на складе.

explain analyze
select *
from equipment_on_departure eod 
join equipment e using(eq_id)
where (eod.return_eq = 'T' or eod.eq_count > 1)
and e.eq_cost >= (select 5 * avg(eq_cost) from equipment) --26300 
order by eod.dep_id desc -- мы знаем, что данные вносятся упорядоченно
limit 10;

-- создание индекса
create index on equipment(eq_cost);
create index on equipment_on_departure(return_eq);
create index on equipment_on_departure(eq_count);

explain analyze
select *
from equipment_on_departure eod 
join equipment e using(eq_id)
where (eod.return_eq = 'T' or eod.eq_count > 1)
and e.eq_cost >= 26300 -- заменить на вычисление максимума
order by eod.dep_id desc
limit 10;

--create index on equipment_on_departure(return_eq, dep_id desc);

explain analyze
WITH ed AS (
  SELECT
    e.eq_id
  FROM
    equipment e
  WHERE
    e.eq_cost >= (select 5 * avg(eq_cost) from equipment)--26300
)
(select dep_id 
from equipment_on_departure eod 
where eod.return_eq = 'T' and  eod.eq_id in (table ed)
order by eod.dep_id desc
limit 10)
union all 
(select dep_id 
from equipment_on_departure eod   
where eod.eq_count > 1 and  eod.eq_id in (table ed)
order by eod.dep_id desc
limit 10)
limit 10;

CREATE OR REPLACE PROCEDURE task_5()
  LANGUAGE plpgsql AS
$proc$
DECLARE
   _line text;
   s_avg numeric;
BEGIN 
   s_avg := (select 5 * avg(eq_cost) from equipment);
   FOR _line IN
    EXPLAIN ANALYZE
	WITH ed AS (
	SELECT
	    e.eq_id
	FROM
	  equipment e
	WHERE
	e.eq_cost >= 26300
	)
	(select dep_id 
	from equipment_on_departure eod 
	where eod.return_eq = 'T' and  eod.eq_id in (table ed)
	order by eod.dep_id desc
	limit 10)
	union all 
	(select dep_id 
	from equipment_on_departure eod   
	where eod.eq_count > 1 and  eod.eq_id in (table ed)
	order by eod.dep_id desc
	limit 10)
	limit 10         
   LOOP
      RAISE NOTICE '%', _line;
   END LOOP;
END
$proc$;

CALL task_5();

-- удаление всех индексов
drop index on equipment_eq_cost_idx;
drop index on equipment_on_departure_return_eq_idx;
drop index on equipment_on_departure_eq_count_idx;
------------------------------------------------------Задание №6------------------------------------------------------
-- Вычислить заработную плату сотрудников-садовников за 2023 год, если известно, что она делится между садовниками,
-- выехавшими на данный объект, и составляет 50 % от суммы выполненных работ на объекте. Сравнить планы выполнения 
-- запросов с использованием вычисляемого столбца и без него (с использованием CTE), при выборке части сотрудников,
-- у которых заработная плата выше определенного значения.
analyze;
/*
explain analyze
with S as(
select d.dep_id, round(d.dep_sum/d.emp_cnt*0.5, 2) as sal
from departure d
where extract(year from d.dep_date) = 2023 --and extract(month from d.dep_date) = 11
)
select e.emp_name, sum(s.sal) as sal_emp
from employee e
left join employee_on_departure eod using(emp_id)
join s using(dep_id)
group by emp_id
order by sal_emp;
*/
explain analyze
with S as(
SELECT e.emp_name, SUM(sal) AS sal_emp
FROM employee e
LEFT JOIN employee_on_departure eod USING (emp_id)
JOIN (
    SELECT d.dep_id, ROUND(d.dep_sum / d.emp_cnt * 0.5, 2) AS sal
    FROM departure d
    WHERE EXTRACT(YEAR FROM d.dep_date) = 2023
) s USING (dep_id)
GROUP BY e.emp_id
)
select sal_emp
from s
where sal_emp > 200000;

create index on departure(extract(year from dep_date));

ALTER TABLE EMPLOYEE ADD COLUMN LAST_YEAR_SALARY NUMERIC;

update employee e2
set LAST_YEAR_SALARY = (SELECT SUM(sal) AS sal_emp
						FROM employee e
						LEFT JOIN employee_on_departure eod USING (emp_id)
						JOIN (
						    SELECT d.dep_id, ROUND(d.dep_sum / d.emp_cnt * 0.5, 2) AS sal
						    FROM departure d
						    WHERE EXTRACT(YEAR FROM d.dep_date) = 2023
						) s USING (dep_id)
						where e2.emp_id = e.emp_id
						GROUP BY e.emp_id);

analyze;
explain analyze
select emp_name
from employee
where LAST_YEAR_SALARY > 200000;

-- удаление индекса
drop index departure_date_part_idx;