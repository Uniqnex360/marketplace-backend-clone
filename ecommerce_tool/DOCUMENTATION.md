# MarketLink — Backend Technical Documentation

## Contributors & References

- **Author**: Harisankar S 
- **Reference**: MarketLynxe documentation was consulted for integration and API design.

---

Version: draft
Last updated: 2025-12-05

---

**Project Overview & Introduction**

- **Project name**: MarketLink (backend)
- **Repository location**: `ecommerce_tool` (Django project)
- **Purpose**: Centralized backend to aggregate marketplace data (orders, inventory, pricing), provide analytics, reporting and operational tooling for merchants across marketplaces such as Amazon, Walmart, SellerCloud and ShipStation.
- **What the tool does**:
  - Exposes a REST API for marketplace operations, dashboards, user management and exports (CSV/XLSX).
  - Periodic background synchronization of orders, products and inventory via Celery.
  - External integrations with marketplace APIs and third-party services (SendGrid, SellerCloud, ShipStation).
  - Provides aggregated analytics and exportable reports used by dashboards.
- **High-level problem it solves**: Consolidates data from multiple marketplaces into a single operational and analytics backend to reduce manual reconciliation and enable reporting, revenue and inventory insights.

---

**Technical Stack**

---

## Credentials & 3rd Party Integrations

### Credentials Used (Environment Variables)

| Integration         | Variable Names Used (in `.env` or environment)                | Purpose/Scope                |
|---------------------|---------------------------------------------------------------|------------------------------|
| Django Secret Key   | `SECRET_KEY`                                                  | Django cryptographic secret  |
| MongoDB             | `DATABASE_HOST`, `DATABASE_NAME`                             | DB connection URI/name       |
| Redis               | `REDIS_HOST`, `REDIS_PASSWORD`                              | Redis broker/cache           |
| Amazon SP-API       | `AMAZON_API_KEY`, `AMAZON_SECRET_KEY`, `AMAZON_REFRESH_TOKEN`, `MARKETPLACE_ID`, `SELLER_ID`, `Role_ARN`, `Acccess_Key`, `Secret_Access_Key` | Amazon marketplace sync      |
| Walmart Marketplace | `WALMART_API_KEY`, `WALMART_SECRET_KEY`                      | Walmart API integration      |
| SendGrid            | `SENDGRID_API_KEY`                                            | Transactional email          |
| SellerCloud         | `SELLERCLOUD_USERNAME`, `SELLERCLOUD_PASSWORD`, `SELLERCLOUD_COMPANY_ID`, `SELLERCLOUD_SERVER_ID` | SellerCloud integration      |
| ShipStation         | `SHIPSTATION_API_KEY`, `SHIPSTATION_API_SECRET`              | ShipStation API integration  |

# ENV
#WALMART API KEYS
# WALMART_API_KEY = ""
# WALMART_SECRET_KEY = ""

WALMART_API_KEY =""
WALMART_SECRET_KEY = ""

#AMAZON API KEYS
# AMAZON_API_KEY = ""
# AMAZON_SECRET_KEY = ""
# AMAZON_REFRESH_TOKEN = ""
MARKETPLACE_ID = ""
SELLER_ID = ""


AMAZON_API_KEY = ""
AMAZON_SECRET_KEY =""
AMAZON_REFRESH_TOKEN = ""


Role_ARN = ""
Acccess_Key= ""
Secret_Access_Key = ""



#SELLER CLOUD CREDENTIALS
# === CONFIGURATION ===
SELLERCLOUD_USERNAME = ""
SELLERCLOUD_PASSWORD = ""
SELLERCLOUD_COMPANY_ID =""
SELLERCLOUD_SERVER_ID = ""


# sendgrid settings

SENDGRID_API_KEY=""


# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = ""


#Database settings

DATABASE_NAME = ""
# DATABASE_USER=""
# DATABASE_PASSWORD=""
# DATABASE_HOST = ""

# DATABASE_HOST = ""


# DATABASE_HOST = ""

DATABASE_HOST = ""
SHIPSTATION_API_KEY=""
SHIPSTATION_API_SECRET=""

### 3rd Party Integrations

- **Amazon SP-API**: Fetches orders, products, pricing, and inventory from Amazon Seller Central.
- **Walmart Marketplace API**: Fetches products, inventory, and order data from Walmart.
- **SendGrid**: Sends transactional and notification emails.
- **SellerCloud**: Integrates for order and inventory management.
- **ShipStation**: Used for order fulfillment, shipping label creation, and order status updates.
- **MongoDB**: Main data store for all business entities.
- **Redis**: Used for caching and as a Celery broker.

---

- **Backend technologies**:
  - Python 3.11
  - Django 4.2
  - Gunicorn for WSGI process management
  - Django REST Framework (`djangorestframework`) and JWT (`djangorestframework-simplejwt`)
  - MongoDB accessed via `mongoengine` (ODM) and `pymongo`
  - Celery 5 for background processing
  - Redis used as Celery broker/backend and Django cache

- **Databases**:
  - Primary datastore: MongoDB (connection via `mongoengine.connect()` using `DATABASE_HOST` and `DATABASE_NAME`).
  - Redis: in-memory store for caching and Celery broker. Configured URLs present in `settings.py`.
- **Messaging/queue systems**:
  - Celery with Redis broker (`CELERY_BROKER_URL = 'redis://:foobaredUniqnex@127.0.0.1:6379/0'`).
  - Django cache uses Redis DB 1 according to settings.

- **DevOps tools**:
  - Docker + Dockerfile for containerization.
  - `docker-compose.yml` included for local development (Redis/Celery sections are present but commented). Recommended CI: GitHub Actions or GitLab CI.

---

**System Architecture**

---

## API Usage & Endpoints

### Internal API Endpoints (selected)

| Endpoint (path)                | Method | Purpose/Data Fetched                | Calls External API? (Which) |
|--------------------------------|--------|-------------------------------------|----------------------------|
| `/omnisight/fetchAllorders/`   | GET    | Fetch all orders                    | Amazon, Walmart            |
| `/omnisight/fetchOrderDetails/`| GET    | Fetch order details                 | Amazon, Walmart            |
| `/omnisight/getProductList/`   | GET    | List products                       | Amazon, Walmart            |
| `/omnisight/fetchProductDetails/`| GET  | Product details                     | Amazon, Walmart            |
| `/omnisight/createManualOrder/`| POST   | Create manual order                 | -                          |
| `/omnisight/listManualOrders/` | GET    | List manual orders                  | -                          |
| `/omnisight/signupUser/`       | POST   | Register user                       | -                          |
| `/omnisight/loginUser/`        | POST   | Login, returns JWT                  | -                          |
| `/omnisight/exportOrderReport/`| GET    | Export orders as CSV/XLSX           | -                          |
| `/omnisight/clearcache/`       | POST   | Clear cache key                     | -                          |
| `/omnisight/getBrandList/`     | GET    | List brands                         | -                          |
| `/omnisight/getMarketplaceList/`| GET   | List marketplaces                   | -                          |
| `/omnisight/getProductVariant/`| GET    | List product variants               | -                          |
| `/omnisight/ordersCountForDashboard/`| GET | Dashboard order count             | -                          |


### External APIs Used

- **Amazon SP-API**: Orders, products, pricing, inventory endpoints (see `amazon_operations.py`).
- **Walmart Marketplace API**: Items, inventory, and order endpoints (see `walmart_operations.py`).
- **ShipStation API**: Orders, rates, labels (see `shipstation_operations.py`).
- **SendGrid API**: Email send endpoint.
- **SellerCloud API**: Order and inventory endpoints.

---

- **Deployment architecture**:
  - Containers: `app` (Gunicorn + Django), optional `celery` worker containers, `redis` for broker locally. Production should use managed services (ElastiCache, MongoDB Atlas) or hosted clusters.
- **Networking overview**:
  - Application and worker containers reside in private network. Redis and MongoDB only accessible from application subnets. TLS enforced at LB/ingress.

- **Service-to-service interactions**:
  - `app` ↔ Redis: cache and broker
  - `app` ↔ MongoDB: read/write via `mongoengine`
  - `app` ↔ Celery workers: task enqueuing; workers read from Redis
  - `app`/workers ↔ external marketplace APIs (HTTP)
- **Data flow**:
  1. Periodic Celery tasks pull orders/product data from marketplaces.
  2. Data normalized and persisted to MongoDB.
  3. API endpoints exposed for dashboards and operational actions.
  4. Exports generated either synchronously (small) or via async export jobs.

---

**Entity & Data Model Documentation**

---

## Database Credentials & Schemas

### Database Credentials (Environment Variables)

- `DATABASE_HOST`: MongoDB connection URI (e.g., `mongodb+srv://user:pass@host/db`)
- `DATABASE_NAME`: MongoDB database name

### Database Schemas (Main Collections)

- **Marketplace**: `{ name, url, image_url, country, created_at, updated_at }`
- **Category**: `{ name, parent_category_id, marketplace_id, breadcrumb_path, level, end_level, created_at, updated_at }`
- **Brand**: `{ name, description, website, marketplace_id, marketplace_ids }`
- **Manufacturer**: `{ name, description, website, marketplace_id }`
- **Product**: `{ product_title, sku, asin, price, quantity, ... (see models.py for full list) }`
- **CachedMetrics**: `{ cache_hash, marketplace_id, brand_ids, product_ids, ... }`
- **User**: `{ first_name, last_name, username, email, password, role_id, ... }`
- **Role**: `{ name, description, priority }`
- **Order**: (see operations, not shown in models.py snippet)

**Indexes**: Most collections have indexes on key fields (e.g., `sku`, `asin`, `marketplace_id`).

---

Note: repo uses MongoDB (`mongoengine`). For complete, exact schemas, consult `omnisight/models.py`. Below is an inferred model list and recommendations.

- **Likely entities**:
  - `User` — system/merchant users (created via `createUser` endpoints)
  - `Order` — marketplace orders with `order_id`, `marketplace`, `status`, `items`, `total`, `created_at`, `updated_at`
  - `OrderItem` (embedded) — `sku`, `asin`, `qty`, `price`, `tax`, `shipping`
  - `Product` — SKU/ASIN mapping, variants, titles, categories, cost/pricing
  - `Inventory` — quantity per location/marketplace
  - `MarketplaceAccount` — credentials and metadata per marketplace
  - `ExportJob` — record for jobs that create CSV/XLSX exports
  - `Metric` or aggregated collections — precomputed analytics (sales/day, top-sellers)

- **Relationships**:
  - Product (1) ↔ (many) OrderItems
  - User (1) ↔ (many) Orders (ownership or account-level scope)
  - Product (many) ↔ (many) Marketplaces (marketplace-specific ids)

- **Schema & constraints (recommendations)**:
  - Create unique/indexed fields: (marketplace, order_id), `sku` unique per merchant, `asin` indexed.
  - Use `created_at` / `updated_at` timestamps on main documents.
  - For logs or temporary data, use TTL indexes to enforce retention.

- **ORM notes (mongoengine)**:
  - Use `Document` for main entities and `EmbeddedDocument` for nested items.
  - Use `meta={'indexes': [...]}` to define compound indexes and TTL indexes.
  - Keep migrations and data-compatibility in mind; MongoDB is schemaless, enforce changes in application logic.

---

**Celery Worker Architecture**

- **Tasks (extracted/inferred from `ecommerce_tool/ecommerce_tool/celery.py` commented schedules and operations references)**:
  - `sync_orders` — sync orders across marketplaces
  - `sync_walmart_orders` — Walmart-specific sync
  - `sync_inventry` — inventory sync
  - `sync_products`, `sync_price` — product catalog and price syncs
  - `backfill_missing_shipping_cost` — backfill shipping cost data
  - `fetch_item_tax_from_amazon`, `fetch_item_full_pricing_from_amazon` — fetch taxes and detailed pricing from Amazon
  - Export/report generation tasks (CSV/XLSX)

- **Scheduling**:
  - `django-celery-beat` used for scheduling recurring tasks (crontab-style schedules). Example schedules in commented `celery.py` show 15-min, hourly, 4-hour schedules.

- **Retry logic & error handling**:
  - Use `@shared_task(bind=True, max_retries=..., default_retry_delay=...)` and `self.retry(exc=...)` for transient errors.
  - Exponential backoff recommended for 429 or 5xx responses from third-party APIs. Persist failed attempts metadata for manual review.



**API Documentation**

---

## Endpoints List with Input/Output Format

### Example: Login

- **Endpoint**: `/omnisight/loginUser/`
- **Method**: POST
- **Input**:
  ```json
  { "username": "marketplace@user2gmail.com", "password": "2" }
  ```
- **Output**:
  ```json
  { "access": "<jwt>", "refresh": "<jwt-refresh>" }
  ```

### Example: Fetch All Orders

- **Endpoint**: `/omnisight/fetchAllorders/`
- **Method**: GET
- **Input**: Query params: `start_date`, `end_date`, `page`, `limit`
- **Output**:
  ```json
  { "count": 123, "page": 1, "page_size": 50, "results": [ { ...order... } ] }
  ```

### Example: Create Manual Order

- **Endpoint**: `/omnisight/createManualOrder/`
- **Method**: POST
- **Input**:
  ```json
  { "order_data": { ... } }
  ```
- **Output**:
  ```json
  { "status": "success", "order_id": "..." }
  ```

**See `omnisight/urls.py` and operations for all endpoints and their formats.**

---

Base path: `/omnisight/` (see `omnisight/urls.py`)

Extracted endpoint list (path -> typical HTTP method(s) and brief description). For exact request/response schemas, consult each view in `omnisight/operations/`.

- `checkEmailExistOrNot/` : POST — check if an email exists
- `clearcache/` : POST — clear cache key
- `signupUser/` : POST — register user
- `loginUser/` : POST — login and receive JWT tokens
- `forgotPassword/` : POST — send password reset
- `changePassword/` : POST — change password (auth)
- `updatedRevenueWidgetAPIView/` : GET/POST — revenue widget data
- Product endpoints: `getProductList/`, `getProductCategoryList/`, `getBrandList/`, `fetchProductDetails/`, `getProductVariant/`
- Orders: `fetchAllorders/`, `fetchOrderDetails/`, `downloadOrders/`, `getOrdersBasedOnProduct/`
- Dashboard: `ordersCountForDashboard/`, `totalSalesAmount/`, `getSalesTrendPercentage/`, `fetchSalesSummary/`, `salesAnalytics/`, `mostSellingProducts/`, `fetchTopSellingCategories/`
- Manual orders: `createManualOrder/`, `listManualOrders/`, `fetchManualOrderDetails/`, `updateManualOrder/`
- Inventory/reports: `fetchInventryList/`, `exportOrderReport/`
- User management: `createUser/`, `updateUser/`, `listUsers/`, `fetchUserDetails/`, `fetchRoles/`
- Filters and analytics endpoints: many `get*` and `download*` endpoints for exports and Helium10 dashboard integrations (see `omnisight/urls.py` for full list).

Authentication & Authorization:
- JWT (SimpleJWT) is present in `INSTALLED_APPS`. Protect endpoints with authentication classes in DRF views. Admin UI available at `/admin/`.

---

## Authentication Methods

- **User Authentication**: JWT (JSON Web Token) via `djangorestframework-simplejwt`.
  - Login endpoint returns `access` and `refresh` tokens.
  - Use `Authorization: Bearer <access_token>` header for authenticated requests.
- **API Keys**: Used for 3rd party integrations (Amazon, Walmart, ShipStation, SellerCloud, SendGrid).
  - API keys are stored in environment variables and injected at runtime.
- **OAuth**: Used for some marketplace integrations (e.g., Walmart, Amazon SP-API refresh tokens).
- **Admin Login**: Django admin available at `/admin/` (requires superuser credentials, not included in repo).

---

## API Keys (Variable Names)

- **Amazon**: `AMAZON_API_KEY`, `AMAZON_SECRET_KEY`, `AMAZON_REFRESH_TOKEN`, `MARKETPLACE_ID`, `SELLER_ID`, `Role_ARN`, `Acccess_Key`, `Secret_Access_Key`
- **Walmart**: `WALMART_API_KEY`, `WALMART_SECRET_KEY`
- **ShipStation**: `SHIPSTATION_API_KEY`, `SHIPSTATION_API_SECRET`
- **SendGrid**: `SENDGRID_API_KEY`
- **SellerCloud**: `SELLERCLOUD_USERNAME`, `SELLERCLOUD_PASSWORD`, `SELLERCLOUD_COMPANY_ID`, `SELLERCLOUD_SERVER_ID`

---


**Environment Setup**

- **Local prerequisites**:
  - Python 3.11
  - MongoDB (local or remote)
  - Redis (local or remote)

- **Quick start (local, virtualenv)**:
  ```bash
  python -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  cp .env.example .env   # create your env file with values
  export DJANGO_SETTINGS_MODULE=ecommerce_tool.settings
  python manage.py runserver
  ```

- **Docker (local)**:
  - `docker-compose.yml` contains an `app` service; `redis` and `celery` services are present but commented. To run locally with Redis and Celery, uncomment the Redis and Celery blocks and run:
  ```bash
  docker-compose up --build
  ```

- **Environment variables (summary)**:
  - `SECRET_KEY` — Django secret
  - `DEBUG` — dev flag
  - `DATABASE_HOST` — MongoDB connection URI
  - `DATABASE_NAME` — MongoDB DB name
  - `CELERY_BROKER_URL` — Redis url
  - `WALMART_API_KEY`, `WALMART_SECRET_KEY`
  - `AMAZON_API_KEY`, `AMAZON_SECRET_KEY`, `AMAZON_REFRESH_TOKEN`, `MARKETPLACE_ID`, `SELLER_ID`
  - `Role_ARN`, `Acccess_Key`, `Secret_Access_Key` — AWS credentials
  - `SENDGRID_API_KEY` — SendGrid
  - `SELLERCLOUD_*` — SellerCloud credentials
  - `SHIPSTATION_API_KEY`, `SHIPSTATION_API_SECRET`
  - `REDIS_HOST`, `REDIS_PASSWORD` (if used)

- **Config differences by environment**:
  - Development: `DEBUG=True`, local DB/Redis or dockerized services.
  - Staging: `DEBUG=False`, staging DB/Redis, separate accounts for marketplaces.
  - Production: `DEBUG=False`, secrets in secret manager, TLS, managed DB/Redis.

---


---

**Security Considerations**

- **API security**:
  - Enforce HTTPS at ingress.
  - Use JWT for authentication and RBAC for authorization.
  - Rate-limit endpoints to mitigate abuse.
- **Secrets management**:
  - Never store secrets in VCS. Use secret managers (AWS Secrets Manager, HashiCorp Vault) and inject at runtime.
- **Access control**:
  - Least privilege for DB/service accounts. Admin UI restricted to trusted IP addresses or via VPN.
- **Data encryption**:
  - TLS in transit for all services. At-rest encryption for DB volumes and backups.
- **Compliance**:
  - Implement data minimization and deletion policies for PII. Maintain audit logs for access.

---

**Maintenance & Observability**

- **Logging**:
  - Structured JSON logs to stdout/stderr and shipped to ELK/CloudWatch/Datadog.
  - Capture request id and user id in logs.

---

**Appendices**

- **Glossary**:
  - Celery: background task queue used for periodic and async jobs.
  - MongoEngine: Python ODM for MongoDB.
  - Omnisight: Django app in this repo containing the core API and operations.

- **Troubleshooting (common issues)**:
  - "Cannot connect to MongoDB": verify `DATABASE_HOST`, network rules, and credentials.
  - "Redis connection refused": ensure Redis is running and `CELERY_BROKER_URL` is correct.
  - Celery tasks failing with 429: implement rate limiting/backoff for marketplace APIs.
  - CORS/CSRF errors: update `CSRF_TRUSTED_ORIGINS` and `CORS_ALLOWED_ORIGINS` in `settings.py`.
  - Gunicorn timeouts: increase `--timeout` and move long-running tasks to Celery.

- **Runbook snippets**:
  - Running Django dev server:
    ```bash
    source .venv/bin/activate
    export DJANGO_SETTINGS_MODULE=ecommerce_tool.settings
    python manage.py runserver
    ```

    ```

---

