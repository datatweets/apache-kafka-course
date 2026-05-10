-- ============================================================
--  MySQL init script — runs automatically on first start
--  Used in Module 9: Kafka Connect (MySQL → Elasticsearch)
-- ============================================================

USE kafka_course;

CREATE TABLE IF NOT EXISTS customers (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(100)  NOT NULL,
    email       VARCHAR(150)  NOT NULL,
    country     VARCHAR(50)   NOT NULL,
    created_at  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT           NOT NULL,
    product     VARCHAR(100)  NOT NULL,
    amount      DECIMAL(10,2) NOT NULL,
    status      VARCHAR(20)   DEFAULT 'pending',
    created_at  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

-- Seed data for demos
INSERT INTO customers (name, email, country) VALUES
    ('Alice Tan',    'alice@example.com',   'Singapore'),
    ('Bob Lim',      'bob@example.com',     'Malaysia'),
    ('Carol Wong',   'carol@example.com',   'Australia'),
    ('David Chen',   'david@example.com',   'Singapore'),
    ('Eva Martinez', 'eva@example.com',     'USA');

INSERT INTO orders (customer_id, product, amount, status) VALUES
    (1, 'Kafka Book',       49.99,  'completed'),
    (2, 'Python Course',    99.00,  'pending'),
    (3, 'Cloud Subscription', 199.00, 'completed'),
    (4, 'Kafka Training',   499.00, 'pending'),
    (5, 'Docker Workshop',  149.00, 'completed');