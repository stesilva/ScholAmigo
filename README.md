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

3. Clone the repository containing the Docker folder:

```bash
git clone https://github.com/stesilva/ScholAmigo
cd <repository-folder>
```


---

### **Step-by-Step Instructions**

### **Notes for Windows Users**

1. It is possible that `chromium:arm64` and `chromium-driver:arm64` in the Dockerfile will not work for Windows; therefore, so Chrome and Chrome driver will need to be installed manually.
2. After that, the script for scraping_daad.py needs to be changed with relevant paths:
   
```python
options.binary_location = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
...
driver = webdriver.Chrome(executable_path=r"C:\chromedriver\chromedriver.exe", options=options)
```

#### **Step 1: Create Required Directories**

For Linux-based OS run the following command to create directories for Airflow:

```bash
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
For other operating systems, you may get a warning that AIRFLOW_UID is not set, but you can safely ignore it. You can also manually create an .env file in the same folder as docker-compose.yaml with this content to get rid of the warning:

```bash
AIRFLOW_UID=50000
```

#### **Step 2: Place Files in the Appropriate Folders**

- Copy DAG files → Place them in the `./dags` folder.
- Final structure of the directory should look like this:

```
Docker/
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
├── sql/
│   ├── create_queries.sql
│   ├── insert_queries.sql
├── kafka_consumer.py
├── requirements.txt
├── docker-compose.yaml
├── Dockerfile
├── Dockerfile-consumer
```

#### **Step 3: Build and Run Docker Compose**

Navigate to the main folder (with docler-compose.yaml) and run these commands to build and start your Airflow environment:

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

---

### **Troubleshooting**

Ensure sufficient system resources are allocated to Docker (Airflow recommends allocating 10GB of RAM for Docker, according to instructions provided in this link: [https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)).

---

### Final Notes

If you encounter any issues, feel free to open an issue on the GitHub repository.
