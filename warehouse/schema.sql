-- RTV Data Warehouse Schema

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- Core dimension tables
CREATE TABLE villages (
    village_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    district VARCHAR(100),
    region VARCHAR(100),
    coordinates GEOMETRY(Point, 4326),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE households (
    household_id VARCHAR(50) PRIMARY KEY,
    village_id VARCHAR(50) REFERENCES villages(village_id),
    household_code VARCHAR(50) UNIQUE,
    head_of_household VARCHAR(100),
    household_size INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE survey_rounds (
    round_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    start_date DATE,
    end_date DATE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE indicators (
    indicator_id VARCHAR(50) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    unit VARCHAR(50),
    target_value NUMERIC,
    is_positive BOOLEAN, -- Whether higher values indicate improvement
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Fact tables
CREATE TABLE household_surveys (
    survey_id SERIAL PRIMARY KEY,
    household_id VARCHAR(50) REFERENCES households(household_id),
    round_id VARCHAR(20) REFERENCES survey_rounds(round_id),
    survey_date DATE NOT NULL,
    surveyor_id VARCHAR(50),
    status VARCHAR(20) DEFAULT 'completed',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(household_id, round_id)
);

CREATE TABLE household_measurements (
    measurement_id SERIAL PRIMARY KEY,
    survey_id INTEGER REFERENCES household_surveys(survey_id),
    indicator_id VARCHAR(50) REFERENCES indicators(indicator_id),
    value NUMERIC,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(survey_id, indicator_id)
);

-- Audit and logging tables
CREATE TABLE data_quality_logs (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    check_type VARCHAR(50),
    check_description TEXT,
    status VARCHAR(20),
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pipeline_logs (
    log_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    run_id VARCHAR(50),
    status VARCHAR(20),
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Views for common queries
CREATE VIEW household_progress AS
SELECT 
    h.household_id,
    h.household_code,
    v.name as village_name,
    v.district,
    v.region,
    sr.name as survey_round,
    sr.start_date as round_start_date,
    i.name as indicator_name,
    i.category as indicator_category,
    hm.value as measurement_value,
    i.unit as measurement_unit
FROM households h
JOIN villages v ON h.village_id = v.village_id
JOIN household_surveys hs ON h.household_id = hs.household_id
JOIN survey_rounds sr ON hs.round_id = sr.round_id
JOIN household_measurements hm ON hs.survey_id = hm.survey_id
JOIN indicators i ON hm.indicator_id = i.indicator_id;

-- Indexes
CREATE INDEX idx_household_surveys_household_id ON household_surveys(household_id);
CREATE INDEX idx_household_surveys_round_id ON household_surveys(round_id);
CREATE INDEX idx_household_measurements_survey_id ON household_measurements(survey_id);
CREATE INDEX idx_household_measurements_indicator_id ON household_measurements(indicator_id);
CREATE INDEX idx_villages_coordinates ON villages USING GIST(coordinates);

-- Functions
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
CREATE TRIGGER update_households_updated_at
    BEFORE UPDATE ON households
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_indicators_updated_at
    BEFORE UPDATE ON indicators
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_household_surveys_updated_at
    BEFORE UPDATE ON household_surveys
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_household_measurements_updated_at
    BEFORE UPDATE ON household_measurements
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
