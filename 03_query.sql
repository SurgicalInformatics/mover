-- !preview conn=sc

-- ! select count(distinct(PAT_ID)) FROM flowsheet

select * FROM flowsheet LIMIT 10

-- ! select * from flowsheet where OR_CASE_ID = '01e0985e08f5de' limit 100

-- ! select distinct(OR_CASE_ID) from flowsheet where OR_CASE_ID like '0b9%' limit 100

-- ! select distinct(LOG_ID) from flowsheet where LOG_ID like '0b9%' limit 100

-- ! select * from flowsheet where PAT_ID like '6d7486%' limit 100

-- ! select min(IN_OR_DTTM) from flowsheet limit 100