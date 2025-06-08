DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'funding_cat') THEN
        CREATE TYPE funding_cat AS ENUM ('fully funded', 'partially funded', 'not specified');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'prog_level') THEN
        CREATE TYPE prog_level AS ENUM
            ('bachelor','master','phd','postdoctoral researchers','faculty');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'req_level') THEN
        CREATE TYPE req_level  AS ENUM
            ('high school diploma','bachelor','master','phd',
             'postdoctoral researchers','faculty');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'intake_seas') THEN
        CREATE TYPE intake_seas AS ENUM ('spring','fall', 'not specified');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'sch_status') THEN
        CREATE TYPE sch_status  AS ENUM ('open','closed', 'not specified');
    END IF;
END $$;

DROP TABLE IF EXISTS
      origin_country,
      program_country,
      scholarship_field_of_study,
      scholarship_info,
      scholarship_filter,
      countries,
      field_of_study
CASCADE;


-- for fast filters
CREATE TABLE scholarship_filter (
    scholarship_id   VARCHAR PRIMARY KEY,
    funding_category funding_cat  NOT NULL,
    program_level    prog_level   NOT NULL,
    required_level   req_level    NOT NULL,
    intake           intake_seas  NOT NULL,
    status           sch_status   NOT NULL
);


CREATE TABLE scholarship_info (
    scholarship_id       VARCHAR PRIMARY KEY
                         REFERENCES scholarship_filter(scholarship_id)
                         ON DELETE CASCADE,
    scholarship_name     TEXT,

    description          TEXT,
    program_country      TEXT[],          
    origin_country       TEXT[],          
    documents            TEXT,
    website              TEXT,
    last_updated_utc     TIMESTAMP,
    deadline             DATE,
    scholarship_coverage TEXT[],
    language_certificates TEXT[],
    fields_of_study_code TEXT[],
    study_duration       TEXT,
    gpa                  TEXT,
    list_of_universities TEXT[],
    other_information    TEXT[]
);

ALTER TABLE scholarship_info
ADD CONSTRAINT chk_fos_codes
CHECK (
    fields_of_study_code IS NULL
    OR fields_of_study_code <@ ARRAY[
         '00','01','02','03','04','05','06','07','08','09','10'
       ]::TEXT[]
);

CREATE TABLE field_of_study (
    field_code CHAR(2) PRIMARY KEY,
    category   TEXT NOT NULL
);

INSERT INTO field_of_study(field_code,category) VALUES
 ('00','Generic programmes and qualifications'),
 ('01','Education'),
 ('02','Arts and humanities'),
 ('03','Social sciences, journalism and information'),
 ('04','Business, administration and law'),
 ('05','Natural sciences, mathematics and statistics'),
 ('06','Information and Communication Technologies (ICTs)'),
 ('07','Engineering, manufacturing and construction'),
 ('08','Agriculture, forestry, fisheries and veterinary'),
 ('09','Health and welfare'),
 ('10','Services');

CREATE TABLE countries (
    country_id SERIAL  PRIMARY KEY,
    name       TEXT    UNIQUE
);
CREATE TABLE scholarship_field_of_study (
    scholarship_id VARCHAR REFERENCES scholarship_filter ON DELETE CASCADE,
    field_code     CHAR(2) REFERENCES field_of_study     ON DELETE RESTRICT,
    PRIMARY KEY (scholarship_id, field_code)
);

CREATE TABLE program_country (
    scholarship_id VARCHAR REFERENCES scholarship_filter ON DELETE CASCADE,
    country_id     INT     REFERENCES countries          ON DELETE RESTRICT,
    PRIMARY KEY (scholarship_id,country_id)
);

CREATE TABLE origin_country (
    scholarship_id VARCHAR REFERENCES scholarship_filter ON DELETE CASCADE,
    country_id     INT     REFERENCES countries          ON DELETE RESTRICT,
    PRIMARY KEY (scholarship_id,country_id)
);


CREATE INDEX IF NOT EXISTS idx_prog_country_fk  ON program_country (country_id);
CREATE INDEX IF NOT EXISTS idx_orig_country_fk  ON origin_country  (country_id);

CREATE INDEX IF NOT EXISTS ix_info_cov_gin    ON scholarship_info
    USING gin (scholarship_coverage);
CREATE INDEX IF NOT EXISTS ix_info_lang_gin   ON scholarship_info
    USING gin (language_certificates);
