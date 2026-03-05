/* ==========================================================================
 * This SQL script defines the default schema for a banking application.
 * It includes tables for customers, accounts, and transactions.
 * POSTGRESQL
 ==========================================================================*/

CREATE TABLE customers (
	customer_id SERIAL PRIMARY KEY,
	firstname VARCHAR(50) NOT NULL,
	lastname VARCHAR(50) NOT NULL,
	email VARCHAR(255) NOT NULL,
	create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE accounts (
	account_id SERIAL PRIMARY KEY,
	customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
	account_type VARCHAR(50) NOT NULL,
	balance NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
	currency CHAR(3) NOT NULL DEFAULT 'EUR',
	create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE transactions (
	transaction_id BIGSERIAL PRIMARY KEY,
	account_id INT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
	transaction_type VARCHAR(50) NOT NULL,
	amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
	recipient_account_id INT NOT NULL,
	status CHAR(20) NOT NULL DEFAULT 'COMPLETED',
	create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
