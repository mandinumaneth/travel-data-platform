CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    date_of_birth DATE NOT NULL,
    country_of_residence VARCHAR(100) NOT NULL,
    phone VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS policies (
    policy_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    policy_number VARCHAR(50) UNIQUE NOT NULL,
    destination_country VARCHAR(100) NOT NULL,
    trip_start_date DATE NOT NULL,
    trip_end_date DATE NOT NULL,
    coverage_type VARCHAR(50) NOT NULL CHECK (coverage_type IN ('medical', 'cancellation', 'baggage', 'comprehensive')),
    premium_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('active', 'expired', 'cancelled')),
    broker_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id SERIAL PRIMARY KEY,
    policy_id INTEGER NOT NULL REFERENCES policies(policy_id),
    claim_type VARCHAR(50) NOT NULL CHECK (claim_type IN ('medical', 'cancellation', 'baggage', 'delay')),
    claim_amount DECIMAL(10,2) NOT NULL,
    claim_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'approved', 'rejected')),
    description TEXT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);
