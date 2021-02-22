begin;
INSERT INTO public.daily_covid19_reports
(confirmed, recovered, hospitalized, deaths, new_confirmed, new_recovered, new_hospitalized, new_deaths, update_date, "source", dev_by, server_by)
select daily_covid19_reports.confirmed as confirmed,
	   daily_covid19_reports.recovered as recovered,
	   daily_covid19_reports.hospitalized as hospitalized,
	   daily_covid19_reports.deaths as deaths,
	   daily_covid19_reports.new_confirmed as new_confirmed,
	   daily_covid19_reports.new_recovered as new_recovered,
	   daily_covid19_reports.new_hospitalized as new_hospitalized,
	   daily_covid19_reports.new_deaths as new_deaths,
	   current_timestamp as update_date,
	   daily_covid19_reports."source" as "source",
	   'tanakrit'::varchar as dev_by,
	   'hosos'::varchar as server_by
from daily_covid19_reports limit 1;
INSERT INTO public.daily_covid19_reports
(confirmed, recovered, hospitalized, deaths, new_confirmed, new_recovered, new_hospitalized, new_deaths, update_date, "source", dev_by, server_by)
select daily_covid19_reports.confirmed as confirmed,
	   daily_covid19_reports.recovered as recovered,
	   daily_covid19_reports.hospitalized as hospitalized,
	   daily_covid19_reports.deaths as deaths,
	   daily_covid19_reports.new_confirmed as new_confirmed,
	   daily_covid19_reports.new_recovered as new_recovered,
	   daily_covid19_reports.new_hospitalized as new_hospitalized,
	   daily_covid19_reports.new_deaths as new_deaths,
	   asdasd as update_date,
	   daily_covid19_reports."source" as "source",
	   'tanakrit'::varchar as dev_by,
	   'hosos'::varchar as server_by
from daily_covid19_reports limit 1;
COMMIT;