Create table District(
	district_id numeric(18,0),
	district_name varchar(255),
);

Create table Town(
	town_id numeric(18,0),
	district_id numeric(18,0),
	town_name varchar(255),
	popularity_index numeric(18,0)
);

Create table Outlet(
	outlet_id numeric(18,0),
	town_id numeric(18,0),
	long varchar(255),
	lat varchar(255),
	popularity_index numeric(18,0),
	age numeric(18,0),
	employee_count numeric(18,0),
);

Create table Employee(
	employee_id numeric(18,0),
	employee_name varchar(255),
);

Create table Customer(
	customer_id numeric(18,0),
	customer_name varchar(255),
	customer_age numeric(18,0),
);

Create table MenuItems(
	menu_item_id numeric(18,0),
	menu_item_name varchar(255),
	menu_item_category varchar(255),
	price float,
);

Create table Year(
	year_id numeric(18,0),
	year_name numeric(18,0)
);

Create table Month(
	month_id varchar(255),
	year_id numeric(18,0),
	season_id numeric(18,0),
	month_number numeric(18,0),
	month_name varchar(255)
);

Create table Quarter(
	quarter_id numeric(18,0),
	year_id numeric(18,0),
	quarter_name varchar(255)
);

Create table Date(
	date_id date,
	month_id varchar(255),
	year_id numeric(18,0),
	weekday bit,
	day_number numeric(18,0),
	day_name varchar(255)
);

Create table Orders(
	order_id numeric(18,0),
	outlet_id numeric(18,0),
	order_type varchar(255),
	date_id date,
);

Create table OperatingCosts(
	outlet_id numeric(18,0),
	month_id varchar(255),
	staff_cost float,
	operation_cost float,
);

Create table Taxes(
	outlet_id numeric(18,0),
	quarter_id numeric(18,0),
	tax float,
);

Create table Sales(
	order_id numeric(18,0),
	menu_item_id numeric(18,0),
	outlet_id numeric(18,0),
	employee_id numeric(18,0),
	customer_id numeric(18,0),
	date_id date,
	sales_amount float
);

Create table SalesTargets(
	outlet_id numeric(18,0),
	menu_item_id numeric(18,0),
	month_id varchar(255),
	sales_target decimal(18,0),
);

Create table CustomerVisits(
	outlet_id numeric(18,0),
	customer_id numeric(18,0),
	month_id varchar(255),
	customer_visit decimal(18,0),
	customer_visit_target decimal(18,0),
);



Create table Season(
	season_id numeric(18,0),
	season_name varchar(255)
);