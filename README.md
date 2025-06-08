To reproduce the project, follow the instructions below.

### **Prerequisites**

1. Install **Docker**:
   - Download Docker from [https://www.docker.com/](https://www.docker.com/).
   - Verify the installation:

```bash
docker --version
```

2. Install **Docker Compose**:
   - Included with Docker Desktop for Mac and Windows.
   - Verify the installation:

```bash
docker-compose --version
```

3. Clone the repository:

```bash
git clone https://github.com/stesilva/ScholAmigo
cd ScholAmigo
```

---

### **Step-by-Step Instructions**

### **Notes for Windows Users**

1. It is possible that `chromium:arm64` and `chromium-driver:arm64` in the Dockerfile will not work for Windows; therefore, Chrome and Chrome driver will need to be installed manually.
2. After that, the script for scraping_daad.py needs to be changed with the relevant paths:

```python
options.binary_location = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
...
driver = webdriver.Chrome(executable_path=r"C:\chromedriver\chromedriver.exe", options=options)
```

#### **Step 1: Create Required Directories**

For Linux-based OS run the following command to create directories for Airflow:

```bash
mkdir -p ./dags ./logs ./plugins ./aws ./scripts ./sql ./outputs
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

For other operating systems, you may get a warning that AIRFLOW_UID is not set, but you can safely ignore it. You can also manually create an .env file in the same folder as docker-compose.yaml with this content to get rid of the warning:

```bash
AIRFLOW_UID=50000
```

#### **Step 2: Place Files in the Appropriate Folders**

- Copy DAG files → Place them in the `./dags` folder.
- Create folder called aws and place config and credentials files (sent by email)
- Final structure of the directory should look like this:

```
ScholAmigo/
├── dags/
│   ├── airflow_batch_daad.py
│   ├── ...
├── logs/
├── plugins/
├── aws/
│   ├── config
│   ├── credentials
├── scripts/
│   ├── entrypoint.sh
│   ├── trusted_zone_daad.py
│   ├── example_script.py
├── sql/
│   ├── create_queries.sql
│   ├── insert_queries.sql
├── outputs/
│   ├── ...
├── kafka_consumer.py
├── requirements.txt
├── docker-compose.yaml
├── Dockerfile
├── Dockerfile-consumer
```

#### **Step 3: Build and Run Docker Compose**

Navigate to the main folder (with docker-compose.yaml) and run these commands to build and start your Airflow environment:

```bash
docker-compose build
docker-compose up airflow-init
docker-compose up
```

#### **Step 4: Access Airflow**

- Open your browser and navigate to [http://localhost:8080](http://localhost:8080).
- Use the following credentials to log in:
  - **Username**: airflow
  - **Password**: airflow
- Manually trigger the DAGs for demonstration

#### **Step 5: Access Kafka Messages**

To visually see messages produced by Kafka producers, open [http://localhost:9021](http://localhost:9021).

#### **Step 6: Access Neo4j Graph**

To visualize the graph that is loaded after triggering the DAG 'load_neo4j, open [http://localhost:7474](http://localhost:7474).

---

### **Additional Notes**

- Spark transformations will need to be executed **outside the container**.  
  To replicate, you will need Spark and Hadoop jars installed locally.
- Airflow did not work with Spark in this setup. As a workaround, for Mac users, you can schedule the example script for the DAAD trusted zone (under the `scripts` folder) using a cron job. This demonstrates that scheduling is possible even without Airflow.
- Define API keys for AWS, Pinecone, and Gemini in the appropriate configuration files or environment variables.

---

### **Troubleshooting**

Ensure sufficient system resources are allocated to Docker (Airflow recommends allocating 10GB of RAM for Docker, according to instructions provided in this link: [https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)).

---

### Example Output
In the folder 'outputs', we present the generated files from running the pipeline. These files demonstrate the structure of the data after it has been extracted from the data sources. Furthermore, these files are also stored in the Amazon S3 buckets created for this project.

---

### **Exploitation Zone Applications**

To execute some of the exploitation zone applications, you can:

1. Run the files `user_alumni_recommendation` or `user_analytics` as examples. These scripts demonstrate how to use the processed data for recommendations and analytics.
2. Trigger 'stream' DAG to produce Kafka messages, and observe Kafka consumer's ouputs on the Terminal, where you can see messages with scholarship recommendations whenever consumer receives 'Save' button clicks.
---

### Final Notes

If you encounter any issues, feel free to open an issue on the GitHub repository.
