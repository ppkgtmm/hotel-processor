CREATE TABLE dim_roomtype (
    id INT IDENTITY(1,1) PRIMARY KEY,
    _id INT,
    name VARCHAR(255),
    price FLOAT4,
    effective_from TIMESTAMP,
    effective_until TIMESTAMP
);

CREATE TABLE dim_time (
    id INT PRIMARY KEY,
    hour SMALLINT,
    minute SMALLINT,
    second SMALLINT 
);

CREATE TABLE dim_date (
    id INT PRIMARY KEY,
    date DATE,
    month SMALLINT,
    quarter SMALLINT,
    year INT
);

CREATE TABLE dim_addon (
    id INT IDENTITY(1,1) PRIMARY KEY,
    _id INT,
    name VARCHAR(255),
    price FLOAT4,
    effective_from TIMESTAMP,
    effective_until TIMESTAMP
);

CREATE TABLE dim_location (
    id INT IDENTITY(1,1) PRIMARY KEY,
    _id INT,
    state VARCHAR(255),
    country VARCHAR(255),
    effective_from TIMESTAMP,
    effective_until TIMESTAMP
);

CREATE TABLE dim_guest (
    id INT IDENTITY(1,1) PRIMARY KEY,
    _id INT,
    email VARCHAR(255),
    dob DATE,
    gender VARCHAR(25),
    effective_from TIMESTAMP,
    effective_until TIMESTAMP
);

CREATE TABLE fct_booking (
    id INT IDENTITY(1,1) PRIMARY KEY,
    date INT,
    guest INT,
    guest_location INT,
    roomtype INT,
    FOREIGN KEY (date) REFERENCES dim_date(id),
    FOREIGN KEY (guest) REFERENCES dim_guest(id),
    FOREIGN KEY (guest_location) REFERENCES dim_location(id),
    FOREIGN KEY (roomtype) REFERENCES dim_roomtype(id)
);

CREATE TABLE fct_amenities (
    id INT IDENTITY(1,1) PRIMARY KEY,
    date INT,
    time INT,
    guest INT,
    guest_location INT,
    roomtype INT,
    addon INT,
    addon_quantity INT,
    FOREIGN KEY (date) REFERENCES dim_date(id),
    FOREIGN KEY (time) REFERENCES dim_time(id),
    FOREIGN KEY (guest) REFERENCES dim_guest(id),
    FOREIGN KEY (guest_location) REFERENCES dim_location(id),
    FOREIGN KEY (roomtype) REFERENCES dim_roomtype(id),
    FOREIGN KEY (addon) REFERENCES dim_addon(id)
);
