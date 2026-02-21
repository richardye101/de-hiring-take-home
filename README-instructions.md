# Hometask - ETL Pipeline

## Getting Started

Please follow the [fork and pull request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects) workflow:

1. Fork the repository.
2. Create a new branch for your solution.
3. Create your pipeline code, documentation.
   - Use `uv` to manage the project and your submission is expected to include `pyproject.toml` and `uv.lock`.
   - Use python `3.13+`.
   - Follow requirements stated below.
4. Send a pull request.
   - In your PR description, include any notes about your approach, assumptions, or design decisions

## Question: Build a Data Integration Pipeline

### Context

You need to build a data pipeline that:

1. Fetches data from [Wikipedia - Toronto](https://en.wikipedia.org/wiki/Toronto) and follows links to a limited depth (2 levels max, i.e. starting URL + linked URLs + their linked URLs)
2. Transforms and validates the data
3. Loads it into a staging area
4. Moves clean data to a final destination
5. Includes basic error handling

## Requirements

### Part 1: Data Extraction

- Create a script that fetches data from [Wikipedia - Toronto](https://en.wikipedia.org/wiki/Toronto) and follows links to a limited depth (2 levels)
- Handle rate limiting and retries
- Implement basic error handling
- Consider circular reference handling for link traversal

### Part 2: Data Transformation

- Clean and transform the data (handle nulls, format dates, validate schemas)

### Part 3: Data Loading

- Load data to a staging location (can be CSV, JSON, or a local database)
- Create a final "production" table/view

### Part 4: Documentation

- Add basic logging throughout the pipeline
- Create a README with setup instructions
- Document data schema and transformations

### Tech Stack (use what you're comfortable with)

- Python (required)
- SQL (if using a database)
- Any libraries you prefer (pandas, sqlalchemy, requests, etc.)

## Deliverables

Your pull request should include:

1. **Working, tested code** with all code files
2. **Project configuration files:**
   - `pyproject.toml` (managed with `uv`)
   - `uv.lock`
3. **README.md** with:
   - Setup instructions
   - How to run the pipeline
   - Data schema documentation
   - Assumptions and design decisions
4. **PR description** with a brief explanation of your approach

## Performance Metrics

In our production environment, we process data for thousands of creators daily, requiring efficient data extraction pipelines. While you don't need to process at that scale for this assignment, we'd like to understand your pipeline's performance characteristics.

**Please include in your PR description:**

- **Links processed per minute:** What throughput (links/minute) can your solution achieve?
- **Brief performance notes:** Any observations about bottlenecks or optimization opportunities you identified
