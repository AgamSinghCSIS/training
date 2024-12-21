-- Done using postgreSQL 
-- By Agampaul Singh 


-- Q 1-3
CREATE TABLE EMP (
	emp_id SERIAL Primary Key, 
	first_name VARCHAR(64) NOT NULL,
	last_name  VARCHAR(64),
	email  		VARCHAR(256) NOT NULL,
	phone_number VARCHAR(12) NOT NULL,
	dept_id		INTEGER REFERENCES DEPARTMENT.department_id,
	address_id	INTEGER REFERENCES ADDRESS.address_id,
	blood_group VARCHAR(5),
	dob			DATE,
	doj			DATE,
	dot			DATE,
	created_ts	TIMESTAMP,
	reference	VARCHAR(64),
	role		VARCHAR(32),
	salary		NUMERIC(6,2),
	band		INTEGER,
	reports_to  INTEGER
);


CREATE TABLE DEPARTMENT (
	department_id	SERIAL PRIMARY KEY,
	department_name VARCHAR(256),
	department_code CHAR(4) NOT NULL,
	address_id		INTEGER REFERENCES ADDRESS.add_id,
	dept_head_id 	INTEGER NOT NULL
);

CREATE TABLE ADDRESS (
	add_id	SERIAL PRIMARY KEY,
	country VARCHAR(256),
	state 	VARCHAR(256),
	city 	VARCHAR(256),
	street 	VARCHAR(256),
	zipcode	INTEGER,
	pre_known_add 	JSONB,
	created_ts		TIMESTAMP,
	last_updated_ts	TIMESTAMP
);

-- Q 4-6 
-- Used Chatgpt to generate values to insert into these tables 
-- EMP TABLE
INSERT INTO EMP 
(first_name, last_name, email, phone_number, dept_id, address_id, blood_group, dob, doj, dot, created_ts, reference, role, salary, band, reports_to) 
VALUES
-- Department 1 Employees
('John', 'Doe', 'john.doe1@example.com', '1234567890', 1, 1, 'O+', '1990-01-15', '2015-06-01', NULL, NOW(), 'LinkedIn', 'HR Manager', 7500.00, 4, NULL),
('Jane', 'Smith', 'jane.smith1@example.com', '1234567891', 1, 2, 'A-', '1985-04-20', '2018-07-15', NULL, NOW(), 'Referral', 'HR Specialist', 6500.00, 3, 1),
('Paul', 'Taylor', 'paul.taylor1@example.com', '1234567892', 1, 3, 'B+', '1991-02-10', '2020-03-01', NULL, NOW(), 'Referral', 'Recruiter', 5500.00, 2, 1),
('Emily', 'Clark', 'emily.clark1@example.com', '1234567893', 1, 4, 'O-', '1988-06-18', '2021-01-15', NULL, NOW(), 'LinkedIn', 'HR Assistant', 4000.00, 1, 1),
('Mark', 'Johnson', 'mark.johnson1@example.com', '1234567894', 1, 5, 'AB+', '1993-05-25', '2019-03-20', NULL, NOW(), 'Referral', 'HR Analyst', 4800.00, 2, 1),

-- Department 2 Employees
('Robert', 'Brown', 'robert.brown1@example.com', '2234567890', 2, 6, 'O-', '1992-11-10', '2019-09-10', NULL, NOW(), 'Website', 'Accountant', 6000.00, 3, 6),
('Emily', 'Jones', 'emily.jones1@example.com', '2234567891', 2, 7, 'A+', '1988-03-25', '2020-01-15', NULL, NOW(), 'LinkedIn', 'Finance Manager', 8000.00, 4, 6),
('Chris', 'Evans', 'chris.evans1@example.com', '2234567892', 2, 8, 'B-', '1990-08-14', '2016-07-20', NULL, NOW(), 'Referral', 'Tax Specialist', 7000.00, 3, 6),
('Sophia', 'Adams', 'sophia.adams1@example.com', '2234567893', 2, 9, 'A-', '1993-05-09', '2022-04-10', NULL, NOW(), 'Website', 'Finance Analyst', 5000.00, 2, 6),
('Jack', 'Moore', 'jack.moore1@example.com', '2234567894', 2, 10, 'AB+', '1987-02-28', '2018-12-01', NULL, NOW(), 'LinkedIn', 'Payroll Specialist', 5200.00, 3, 6),

-- Department 3 Employees
('Michael', 'Johnson', 'michael.johnson1@example.com', '3234567890', 3, 11, 'O+', '1995-07-14', '2021-05-30', NULL, NOW(), 'Referral', 'Engineer', 7000.00, 2, 9),
('Sophia', 'Williams', 'sophia.williams1@example.com', '3234567891', 3, 12, 'B-', '1993-10-10', '2017-12-01', NULL, NOW(), 'LinkedIn', 'Engineering Lead', 9000.00, 5, 9),
('David', 'Martinez', 'david.martinez1@example.com', '3234567892', 3, 13, 'A-', '1992-01-28', '2018-09-15', NULL, NOW(), 'Referral', 'Software Engineer', 8500.00, 4, 9),
('Olivia', 'White', 'olivia.white1@example.com', '3234567893', 3, 14, 'O+', '1990-04-10', '2015-12-05', NULL, NOW(), 'LinkedIn', 'Quality Analyst', 6500.00, 3, 9),
('Jake', 'Miller', 'jake.miller1@example.com', '3234567894', 3, 15, 'AB+', '1994-11-18', '2020-01-12', NULL, NOW(), 'Referral', 'System Engineer', 7200.00, 3, 9),

-- Department 4 Employees
('James', 'Miller', 'james.miller1@example.com', '4234567890', 4, 16, 'A+', '1991-02-28', '2016-11-20', NULL, NOW(), 'Referral', 'Marketing Executive', 5500.00, 3, 13),
('Isabella', 'Moore', 'isabella.moore1@example.com', '4234567891', 4, 17, 'O+', '1996-06-05', '2022-02-14', NULL, NOW(), 'Website', 'Marketing Manager', 8500.00, 4, 13),
('Ethan', 'Taylor', 'ethan.taylor1@example.com', '4234567892', 4, 18, 'O-', '1989-09-23', '2017-07-01', NULL, NOW(), 'LinkedIn', 'Content Specialist', 5200.00, 2, 13),
('Sophia', 'Brown', 'sophia.brown1@example.com', '4234567893', 4, 19, 'AB+', '1993-12-12', '2020-10-15', NULL, NOW(), 'Referral', 'SEO Analyst', 4800.00, 1, 13),
('William', 'Davis', 'william.davis1@example.com', '4234567894', 4, 20, 'B-', '1987-03-14', '2014-05-25', NULL, NOW(), 'Referral', 'Advertising Specialist', 6700.00, 3, 13),

-- Department 5 Employees
('Lucas', 'Anderson', 'lucas.anderson1@example.com', '5234567890', 5, 1, 'O+', '1992-06-14', '2020-01-15', NULL, NOW(), 'LinkedIn', 'Operations Manager', 8500.00, 4, 17),
('Emma', 'Hall', 'emma.hall1@example.com', '5234567891', 5, 2, 'A-', '1990-08-10', '2018-04-01', NULL, NOW(), 'Referral', 'Logistics Coordinator', 6000.00, 3, 17),
('Noah', 'Garcia', 'noah.garcia1@example.com', '5234567892', 5, 3, 'B+', '1993-11-20', '2017-06-10', NULL, NOW(), 'Website', 'Warehouse Specialist', 5000.00, 2, 17),
('Ava', 'Lee', 'ava.lee1@example.com', '5234567893', 5, 4, 'AB+', '1988-12-25', '2016-02-05', NULL, NOW(), 'LinkedIn', 'Operations Assistant', 4500.00, 1, 17),
('Mason', 'Lopez', 'mason.lopez1@example.com', '5234567894', 5, 5, 'O-', '1995-04-15', '2021-03-12', NULL, NOW(), 'Referral', 'Inventory Specialist', 5200.00, 2, 17),

-- Department 6 Employees
('Evelyn', 'Clark', 'evelyn.clark1@example.com', '6234567890', 6, 6, 'A+', '1989-05-18', '2016-07-01', NULL, NOW(), 'Referral', 'Research Scientist', 7000.00, 4, 20),
('Liam', 'Harris', 'liam.harris1@example.com', '6234567891', 6, 7, 'B-', '1987-01-24', '2015-09-15', NULL, NOW(), 'LinkedIn', 'Lab Technician', 5500.00, 3, 20),
('Charlotte', 'Young', 'charlotte.young1@example.com', '6234567892', 6, 8, 'O+', '1992-10-14', '2020-08-01', NULL, NOW(), 'Website', 'Research Assistant', 4000.00, 2, 20),
('Benjamin', 'Perez', 'benjamin.perez1@example.com', '6234567893', 6, 9, 'A-', '1994-02-11', '2021-01-10', NULL, NOW(), 'Referral', 'Lab Manager', 6800.00, 4, 20),
('Sophia', 'Nelson', 'sophia.nelson1@example.com', '6234567894', 6, 10, 'AB-', '1988-06-08', '2019-11-25', NULL, NOW(), 'LinkedIn', 'Research Specialist', 5200.00, 3, 20),

-- Department 7 Employees
('Elijah', 'Mitchell', 'elijah.mitchell1@example.com', '7234567890', 7, 11, 'O+', '1991-03-21', '2015-05-01', NULL, NOW(), 'Website', 'IT Manager', 8000.00, 4, 23),
('Mia', 'Carter', 'mia.carter1@example.com', '7234567891', 7, 12, 'A+', '1985-09-12', '2018-10-15', NULL, NOW(), 'LinkedIn', 'System Administrator', 6200.00, 3, 23),
('James', 'Reed', 'james.reed1@example.com', '7234567892', 7, 13, 'B-', '1992-07-18', '2020-03-12', NULL, NOW(), 'Referral', 'Network Engineer', 7000.00, 4, 23),
('Amelia', 'Ward', 'amelia.ward1@example.com', '7234567893', 7, 14, 'O-', '1996-01-09', '2021-06-15', NULL, NOW(), 'LinkedIn', 'Technical Support Specialist', 5000.00, 2, 23),
('Oliver', 'Torres', 'oliver.torres1@example.com', '7234567894', 7, 15, 'A-', '1993-12-03', '2022-09-01', NULL, NOW(), 'Referral', 'Cloud Engineer', 7600.00, 4, 23),

-- Department 8 Employees
('Harper', 'Collins', 'harper.collins1@example.com', '8234567890', 8, 16, 'AB+', '1990-05-15', '2018-04-20', NULL, NOW(), 'LinkedIn', 'Legal Advisor', 8500.00, 4, 26),
('Lucas', 'Cook', 'lucas.cook1@example.com', '8234567891', 8, 17, 'B-', '1986-08-14', '2015-12-01', NULL, NOW(), 'Referral', 'Legal Assistant', 6000.00, 3, 26),
('Ella', 'Bailey', 'ella.bailey1@example.com', '8234567892', 8, 18, 'A-', '1994-09-09', '2020-02-01', NULL, NOW(), 'Website', 'Compliance Specialist', 5800.00, 3, 26),
('William', 'Morgan', 'william.morgan1@example.com', '8234567893', 8, 19, 'O+', '1995-07-07', '2021-07-15', NULL, NOW(), 'Referral', 'Legal Intern', 4500.00, 2, 26),
('Lily', 'King', 'lily.king1@example.com', '8234567894', 8, 20, 'O-', '1989-10-01', '2019-11-20', NULL, NOW(), 'LinkedIn', 'Paralegal', 5000.00, 3, 26);



-- DEPARTMENT TABLE 
INSERT INTO DEPARTMENT (department_name, department_code, address_id, dept_head_id) VALUES
('Human Resources', 'HR01', 1, 101),
('Finance', 'FN02', 2, 102),
('Engineering', 'EN03', 3, 103),
('Marketing', 'MK04', 4, 104),
('Sales', 'SL05', 5, 105),
('Customer Support', 'CS06', 6, 106),
('IT Services', 'IT07', 7, 107),
('Logistics', 'LG08', 8, 108);

SELECT * 
FROM department;

-- Address table 
INSERT INTO ADDRESS (country, state, city, street, zipcode, pre_known_add, created_ts, last_updated_ts) VALUES
('USA', 'California', 'Los Angeles', '123 Elm St', 90001, '{"landmark": "near Central Park", "type": "residential"}', NOW(), NOW()),
('USA', 'New York', 'New York', '456 Maple Ave', 10001, '{"landmark": "near Times Square", "type": "commercial"}', NOW(), NOW()),
('Canada', 'Ontario', 'Toronto', '789 Oak Blvd', 10001, '{"landmark": "near CN Tower", "type": "residential"}', NOW(), NOW()),
('UK', 'England', 'London', '101 Pine Rd', 20001, '{"landmark": "near Tower Bridge", "type": "residential"}', NOW(), NOW()),
('India', 'Maharashtra', 'Mumbai', '202 Cedar Ln', 400001, '{"landmark": "near Gateway of India", "type": "residential"}', NOW(), NOW()),
('Germany', 'Bavaria', 'Munich', '303 Birch St', 80001, '{"landmark": "near Marienplatz", "type": "commercial"}', NOW(), NOW()),
('Australia', 'New South Wales', 'Sydney', '404 Spruce Ave', 2000, '{"landmark": "near Opera House", "type": "residential"}', NOW(), NOW()),
('France', 'Île-de-France', 'Paris', '505 Redwood Rd', 75000, '{"landmark": "near Eiffel Tower", "type": "residential"}', NOW(), NOW()),
('Japan', 'Tokyo', 'Tokyo', '606 Fir Ln', 100-0001, '{"landmark": "near Tokyo Tower", "type": "commercial"}', NOW(), NOW()),
('China', 'Beijing', 'Beijing', '707 Cypress Blvd', 100000, '{"landmark": "near Forbidden City", "type": "residential"}', NOW(), NOW()),
('Brazil', 'São Paulo', 'São Paulo', '808 Sequoia St', 10000, '{"landmark": "near Paulista Avenue", "type": "commercial"}', NOW(), NOW()),
('South Africa', 'Gauteng', 'Johannesburg', '909 Aspen Rd', 2000, '{"landmark": "near Sandton City", "type": "residential"}', NOW(), NOW()),
('Mexico', 'Mexico City', 'Mexico City', '110 Willow Ln', 10000, '{"landmark": "near Zocalo", "type": "residential"}', NOW(), NOW()),
('Italy', 'Lazio', 'Rome', '121 Maple St', 00100, '{"landmark": "near Colosseum", "type": "commercial"}', NOW(), NOW()),
('Russia', 'Moscow', 'Moscow', '132 Elm Blvd', 101000, '{"landmark": "near Red Square", "type": "residential"}', NOW(), NOW()),
('Spain', 'Catalonia', 'Barcelona', '143 Pine Ave', 08000, '{"landmark": "near Sagrada Familia", "type": "residential"}', NOW(), NOW()),
('Netherlands', 'North Holland', 'Amsterdam', '154 Oak St', 1000, '{"landmark": "near Anne Frank House", "type": "residential"}', NOW(), NOW()),
('South Korea', 'Seoul', 'Seoul', '165 Birch Ln', 100-011, '{"landmark": "near N Seoul Tower", "type": "commercial"}', NOW(), NOW()),
('UAE', 'Dubai', 'Dubai', '176 Cedar Rd', 00000, '{"landmark": "near Burj Khalifa", "type": "residential"}', NOW(), NOW()),
('Singapore', 'Central Region', 'Singapore', '187 Redwood Blvd', 00000, '{"landmark": "near Marina Bay Sands", "type": "residential"}', NOW(), NOW());

-- Q.7 Emp table 
ALTER TABLE emp
ALTER COLUMN salary TYPE NUMERIC(8,2);

-- promotion to all employees in department 1
UPDATE emp
SET salary = salary * 1.10 
WHERE dept_id = 1;

SELECT * 
FROM emp
ORDER BY dept_id
LIMIT 10;
	
	
-- Department table 
SELECT department_name, dept_head_id 
FROM department;

-- The Department head for human resources left 
-- the company 
UPDATE department 
SET dept_head_id = 111 
WHERE department_name = 'Human Resources';

SELECT department_name, dept_head_id 
FROM department
ORDER BY department_id;
	
-- Address table 
SELECT * 
FROM address 


UPDATE address 
SET city = 'San Jose', 
    street = 'Santana row',
    Zipcode = 95128,
    last_updated_ts = NOW()
WHERE state = 'California';
	
-- Q.8 Delete question 
-- Emp: Delete rows for payroll specialists as company 
-- implemented Automated software for salary payroll 
SELECT * 
FROM emp 
WHERE role = 'Payroll Specialist';

DELETE FROM EMP 
WHERE role = 'Payroll Specialist';

-- Department: The company is outsourcing the customer 
-- support department. 
- It Violates the foreign key constraint from table 
-- emp, so remove all emps first as they will be layed off 
DELETE FROM EMP 
WHERE dept_id = 6; 

DELETE FROM department
WHERE department_name = 'Customer Support';

SELECT * 
FROM department; 

-- Address: some duplicate address are there, so we 
-- will delete those records 

DELETE FROM address 
WHERE add_id IN (21,22); 

-- Q.9
ALTER TABLE emp 
DROP COLUMN reports_to;

-- Q.10 
ALTER TABLE department 
DROP COLUMN dept_head_id;

-- Q.11 
ALTER TABLE EMP 
ALTER COLUMN band TYPE varchar(16);
		
-- Q.12 
SELECT EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob)) AS emp_age 
FROM emp;

-- Q.13 
SELECT CURRENT_DATE;

-- Q.14 
SELECT EXTRACT(YEAR FROM AGE(CURRENT_DATE, doj)) AS emp_experience 
FROM emp;

-- Q.15 
ALTER TABLE emp
ADD COLUMN comment_col VARCHAR(256); 

-- Q.16 
ALTER TABLE department 
ADD COLUMN comment_col VARCHAR(256);

-- Q.17 
ALTER TABLE address 
ADD COLUMN comment_col VARCHAR(256);

SELECT table_name, column_name
FROM information_schema.columns 
WHERE table_name IN ('address','department','emp')
	AND column_name = 'comment_col';

-- Q.19  
CREATE VIEW employee_details AS 
SELECT 
    CASE WHEN last_name IS NULL THEN first_name 
	ELSE CONCAT(first_name, ' ', last_name)
    END AS name, 
    CONCAT(street,' ',city,' ', state,' ',country,
	' ', zipcode) AS address, 
    department_name AS dept_name, 
    e.salary AS salary
FROM emp AS e
INNER JOIN department AS d
    ON e.dept_id = d.department_id
INNER JOIN address AS a
    ON a.add_id = e.address_id;

SELECT * FROM employee_details; 


-- Q.18 
TRUNCATE emp, department, address;
SELECT * 
FROM emp;

-- Q.20, cascades to created view. 
DROP TABLE emp, department, address CASCADE;

