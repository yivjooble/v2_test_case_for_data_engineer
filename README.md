# ArXiv Paper Processing Pipeline

A comprehensive data engineering pipeline that scrapes, processes, and analyzes academic papers from arXiv using Apache Airflow, PostgreSQL, and OpenAI API.

## Technology Stack
- **Apache Airflow 3.0.2** - Workflow orchestration
- **PostgreSQL 13** - Data storage and analytics
- **OpenAI API** - AI-powered translation and analysis
- **ArXiv API** - Academic paper data source
- **Docker & Docker Compose** - Containerized deployment
- **Python 3.x** - Core programming language

## Quick Start Guide

### Prerequisites
- Docker and Docker Compose installed
- Git installed
- OpenAI API key

### Step-by-Step Setup

#### 1. Clone the Repository
```bash
git clone <repository-url>
cd v2_test_case_for_data_engineer
```

#### 2. Environment Configuration
```bash
# Copy the example environment file
cp .env.example .env

# Edit the .env file with your configuration
# Set your OpenAI API key and Airflow UID
echo "AIRFLOW_UID=$(id -u)" >> .env
echo 'OPENAI_API_KEY="your-openai-api-key-here"' >> .env
```

#### 3. Build and Start Services
```bash
# Build the custom Airflow image
docker-compose build

# Initialize Airflow database
docker-compose up airflow-init

# Start all services
docker-compose up -d
```

#### 4. Verify Installation
```bash
# Check if all services are running
docker-compose ps

# View logs if needed
docker-compose logs airflow-apiserver
```

#### 5. Access Airflow Web UI
- Open your browser and navigate to: `http://localhost:8080`
- Default credentials:
  - Username: `airflow`
  - Password: `airflow`

### Airflow Configuration Setup

After accessing the Airflow web interface, you need to configure the following connections:

#### 1. OpenAI API Connection
1. Create new Airflow variable named `OPENAI_API_KEY`.

#### 2. PostgreSQL Connection
1. Create new Airflow postgres connection named `default_db_postgres`.

## ArXiv DAG Workflow

The `arxiv_paper_processing_v3` DAG performs the following steps:

1. **Create Schema** - Creates the `arxiv` schema in PostgreSQL if it doesn't exist

2. **Create Table** - Creates appropriate tables in the `arxiv` schema

3. **Scrape Papers** - Uses feedparser to fetch recent papers from arXiv API
   - Extracts paper metadata (ID, title, abstract, authors, research field, published date)
   - Normalizes text and maps categories to research fields
   - Filters papers from the last 7 days

4. **Store Papers** - Saves scraped papers to `arxiv_papers` table in PostgreSQL

5. **Parallel Processing**:
   - **Translate Papers** - Uses OpenAI API to translate paper titles and abstracts to Ukrainian
     - Stores translations in `paper_translations` table
     - Handles rate limiting and API errors gracefully
   - **Analyze Papers** - Uses OpenAI API to analyze papers for research classification, key findings, methodology, and impact score
     - Stores analysis results in `paper_analytics` table
     - Provides structured JSON output with scoring metrics

6. **Generate Report** - Creates comprehensive analytics report with statistics and field distribution
   - Stores final report in `analytics_reports` table
   - Logs comprehensive summary to Airflow logs
   - Includes paper count, field distribution, and top research areas
