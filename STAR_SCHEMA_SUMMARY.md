# Star Schema for Payments Data

## Overview
This star schema is designed for analyzing payment transactions, customer behavior, and business metrics for the demopayments system.

## Architecture

### 🌟 **Star Schema Structure**

```
                    dim_date
                       |
                       |
dim_customers ---- fact_transactions ---- dim_payment_methods
                       |
                       |
                  (Additional marts)
```

## 📊 **Data Model**

### **Fact Table**
- **`fact_transactions`** - Central fact table containing transaction events
  - Primary Key: `transaction_id`
  - Foreign Keys: `debtor_customer_id`, `creditor_customer_id`, `payment_method_id`, `transaction_date_key`
  - Measures: `amount`, `completed_amount`, `failed_amount`, `pending_amount`
  - Flags: `is_completed`, `is_failed`, `is_pending`

### **Dimension Tables**

1. **`dim_customers`** - Customer information
   - Primary Key: `customer_id`
   - Attributes: `customer_type`, `email`, `phone_number`, `address`, `kyc_status`, `risk_profile`

2. **`dim_payment_methods`** - Payment method details
   - Primary Key: `payment_method_id`
   - Attributes: `method_type`, `method_category`, `masked_card_number`, `bank_code`, `is_default`

3. **`dim_date`** - Date dimension for time-based analysis
   - Primary Key: `date_key`
   - Attributes: `year`, `quarter`, `month`, `week`, `day_name`, `is_weekend`, `is_weekday`

### **Staging Models**
- **`stg_customers`** - Extracts customer data from JSON payloads
- **`stg_transactions`** - Extracts transaction data from JSON payloads  
- **`stg_payment_methods`** - Extracts payment method data from JSON payloads

### **Mart Tables** (Business Logic Layer)

1. **`mart_daily_transaction_summary`** - Daily aggregated transaction metrics
   - Transaction counts by status
   - Transaction amounts and averages
   - Success rates
   - Unique customer counts

2. **`mart_customer_analysis`** - Customer-level analytics
   - Customer lifetime value
   - Transaction behavior
   - Customer segmentation (One-time, Occasional, Regular, Power)
   - Value segmentation (Low, Medium, High, Premium)

## 🔄 **Data Flow**

```
Raw Airbyte Data (JSON) 
    ↓
Staging Models (Extract & Clean)
    ↓
Dimension Tables (SCD Type 1)
    ↓
Fact Table (Transaction Events)
    ↓
Mart Tables (Business Metrics)
```

## 📈 **Key Business Metrics Available**

### Transaction Analytics
- Daily transaction volumes and amounts
- Success/failure rates
- Average transaction values
- Payment method preferences
- Transaction timing patterns

### Customer Analytics
- Customer segmentation
- Customer lifetime value
- Transaction frequency
- Risk profile analysis
- Geographic distribution

### Operational Analytics
- Payment method performance
- Daily/weekly/monthly trends
- Customer acquisition patterns
- Transaction failure analysis

## 🚀 **Sample Queries**

### Daily Transaction Summary
```sql
SELECT 
    date_day,
    total_transactions,
    total_amount,
    success_rate_percent,
    unique_debtor_customers
FROM `prd-dagen.payments.mart_daily_transaction_summary`
ORDER BY date_day DESC;
```

### Customer Analysis
```sql
SELECT 
    email,
    customer_segment,
    value_segment,
    total_transactions,
    total_transaction_amount
FROM `prd-dagen.payments.mart_customer_analysis`
WHERE total_transactions > 0
ORDER BY total_transaction_amount DESC;
```

### Star Schema Join Example
```sql
SELECT 
    f.amount,
    f.status,
    dc.email as debtor_email,
    dc.risk_profile,
    pm.method_category,
    d.month_name,
    d.year
FROM `prd-dagen.payments.fact_transactions` f
LEFT JOIN `prd-dagen.payments.dim_customers` dc ON f.debtor_customer_id = dc.customer_id
LEFT JOIN `prd-dagen.payments.dim_payment_methods` pm ON f.payment_method_id = pm.payment_method_id
LEFT JOIN `prd-dagen.payments.dim_date` d ON f.transaction_date_key = d.date_key;
```

## 📋 **Current Data Volume**
- **Customers**: 34 records
- **Payment Methods**: 21 records  
- **Transactions**: 21 records
- **Date Dimension**: 2,444 dates (10+ years)
- **Daily Summary**: 1 day of data

## 🔧 **Technical Implementation**
- **Platform**: BigQuery
- **Dataset**: `prd-dagen.payments`
- **Materialization**: Tables for dimensions and facts, Views for staging
- **Data Source**: Airbyte raw tables with JSON payloads
- **ETL Tool**: dbt (Data Build Tool)

## 🎯 **Benefits of This Star Schema**
1. **Fast Query Performance** - Optimized for analytical queries
2. **Easy to Understand** - Business users can easily navigate relationships
3. **Scalable** - Can handle growing transaction volumes
4. **Flexible** - Easy to add new dimensions or measures
5. **Business-Focused** - Designed around key business questions