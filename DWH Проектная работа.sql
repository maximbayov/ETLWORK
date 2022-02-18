
CREATE SCHEMA etlbook;

-- Измерение Календарь

DROP TABLE IF EXISTS etlbook.dim_calendar CASCADE;				

CREATE TABLE etlbook.dim_calendar
AS
WITH dates AS (
    SELECT dd::date AS dt
    FROM generate_series
            ('2010-01-01'::timestamp
            , '2030-01-01'::timestamp
            , '1 day'::interval) dd
)
SELECT
    to_char(dt, 'YYYYMMDD')::int AS id,
    dt AS date,
    to_char(dt, 'YYYY-MM-DD') AS ansi_date,
    date_part('isodow', dt)::int AS day,
    date_part('week', dt)::int AS week_number,
    date_part('month', dt)::int AS month,
    date_part('isoyear', dt)::int AS year,
    (date_part('isodow', dt)::smallint BETWEEN 1 AND 5)::int AS week_day,
    (to_char(dt, 'YYYYMMDD')::int IN (
        20130101,
        20130102,
        20130103,
        20130104,
        20130105,
        20130106,
        20130107,
        20130108,
        20130223,
        20130308,
        20130310,
        20130501,
        20130502,
        20130503,
        20130509,
        20130510,
        20130612,
        20131104,
        20140101,
        20140102,
        20140103,
        20140104,
        20140105,
        20140106,
        20140107,
        20140108,
        20140223,
        20140308,
        20140310,
        20140501,
        20140502,
        20140509,
        20140612,
        20140613,
        20141103,
        20141104,
        20150101,
        20150102,
        20150103,
        20150104,
        20150105,
        20150106,
        20150107,
        20150108,
        20150109,
        20150223,
        20150308,
        20150309,
        20150501,
        20150504,
        20150509,
        20150511,
        20150612,
        20151104,
        20160101,
        20160102,
        20160103,
        20160104,
        20160105,
        20160106,
        20160107,
        20160108,
        20160222,
        20160223,
        20160307,
        20160308,
        20160501,
        20160502,
        20160503,
        20160509,
        20160612,
        20160613,
        20161104,
        20170101,
        20170102,
        20170103,
        20170104,
        20170105,
        20170106,
        20170107,
        20170108,
        20170223,
        20170224,
        20170308,
        20170501,
        20170508,
        20170509,
        20170612,
        20171104,
        20171106,
        20180101,
        20180102,
        20180103,
        20180104,
        20180105,
        20180106,
        20180107,
        20180108,
        20180223,
        20180308,
        20180309,
        20180430,
        20180501,
        20180502,
        20180509,
        20180611,
        20180612,
        20181104,
        20181105,
        20181231,
        20190101,
        20190102,
        20190103,
        20190104,
        20190105,
        20190106,
        20190107,
        20190108,
        20190223,
        20190308,
        20190501,
        20190502,
        20190503,
        20190509,
        20190510,
        20190612,
        20191104,
        20200101, 20200102, 20200103, 20200106, 20200107, 20200108,
       20200224, 20200309, 20200501, 20200504, 20200505, 20200511,
       20200612, 20201104))::int AS holiday
FROM dates
ORDER BY dt;

ALTER TABLE etlbook.dim_calendar ADD PRIMARY KEY (id);

-- Измерение Пассажиры

DROP TABLE IF EXISTS etlbook.dim_passengers CASCADE;

CREATE TABLE etlbook.dim_passengers (
	id serial PRIMARY KEY,
    passenger_key varchar(20) UNIQUE NOT NULL,
    passenger_name text NOT NULL,
    email varchar(250),
    phone varchar(100),
	last_update timestamp NOT NULL DEFAULT now()
);


-- Измерение Самолёты

DROP TABLE IF EXISTS etlbook.dim_aircrafts CASCADE;

CREATE TABLE etlbook.dim_aircrafts (
	id serial PRIMARY KEY,
    aircraft_key varchar(3) UNIQUE NOT NULL,
    model text NOT NULL,
    RANGE integer NOT NULL,
    last_update timestamp NOT NULL DEFAULT now()
);


-- Измерение Аэропорты

DROP TABLE IF EXISTS etlbook.dim_airports CASCADE;

CREATE TABLE etlbook.dim_airports (
	id serial PRIMARY KEY,
    airport_key varchar(3) UNIQUE NOT NULL,
    airport_name varchar(200) NOT NULL,
    city varchar(200) NOT NULL,
    longitude float NOT NULL,
    latitude float NOT NULL,
    timezone TEXT NOT NULL,
    last_update timestamp NOT NULL DEFAULT now()
);

-- Измерение Тарифы

DROP TABLE IF EXISTS etlbook.dim_tariff CASCADE;

CREATE TABLE etlbook.dim_tariff (
    id serial PRIMARY KEY,
    tariff varchar(10) UNIQUE NOT NULL
);


-- Таблица фактов 

DROP TABLE IF EXISTS etlbook.fact_flights CASCADE;

CREATE TABLE etlbook.fact_flights (
    date_departure_key int NOT NULL REFERENCES etlbook.dim_calendar(id),
    date_arrival_key int NOT NULL REFERENCES etlbook.dim_calendar(id),
    passenger_key int NOT NULL REFERENCES etlbook.dim_passengers(id),
    aircraft_key int NOT NULL REFERENCES etlbook.dim_aircrafts(id),        
    airport_departure_key int NOT NULL REFERENCES etlbook.dim_airports(id),
    airport_arrival_key int NOT NULL REFERENCES etlbook.dim_airports(id),
    tariff_key int NOT NULL REFERENCES etlbook.dim_tariff(id),
    actual_departure timestamptz,
    actual_arrival timestamptz,
    departure_delay int,
    arrival_delay int,
    amount numeric(10,2) NOT NULL,
    dt timestamp NOT NULL
);


DROP VIEW IF EXISTS bookings.fact_flights_view;

CREATE VIEW bookings.fact_flights_view AS (
	SELECT 
		to_char(f.actual_departure, 'YYYYMMDD')::int as date_departure_key,
		to_char(f.actual_arrival, 'YYYYMMDD')::int as date_arrival_key,
		t.passenger_id AS passenger_key,
		f.aircraft_code AS aircraft_key,
		f.departure_airport AS airport_departure_key,
		f.arrival_airport AS airport_arrival_key,
		tf.fare_conditions AS tariff_key,
		f.actual_departure,
		f.actual_arrival,
		EXTRACT(EPOCH FROM f.actual_departure::timestamp) - EXTRACT(EPOCH FROM f.scheduled_departure::timestamp)::int AS departure_delay,   -- разница в секундах
		EXTRACT(EPOCH FROM f.actual_arrival::timestamp) - EXTRACT(EPOCH FROM f.scheduled_arrival::timestamp)::int AS arrival_delay,
		tf.amount
	FROM bookings.tickets t 
	LEFT JOIN bookings.ticket_flights tf using(ticket_no)
	LEFT JOIN bookings.flights f using(flight_id)
	WHERE f.actual_departure IS NOT NULL AND f.actual_arrival IS NOT NULL
);



\- проверка

SELECT *
FROM etlbook.fact_flights ff 
LEFT JOIN etlbook.dim_calendar dc ON dc.id = ff.date_departure_key 
WHERE dc."year" = 2016 AND dc."month" = 9;



























