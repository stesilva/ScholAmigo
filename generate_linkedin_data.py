import json
import random
from faker import Faker

faker = Faker()

def generate_profile():
    return {
        'name': faker.name(),
        'url': f"https://www.linkedin.com/in/{faker.user_name()}",
        'headline': random.choice([
            'Big Data Enthusiast | Erasmus Mundus BDMA | Data Scientist',
            'AI & Analytics Specialist | BDMA Graduate | Machine Learning Expert',
            'Data Engineer | Big Data Management | Erasmus Mundus Alumni'
        ]),
        'about': 'Passionate about leveraging Big Data technologies to drive impactful solutions in data science and analytics.',
        'experience': [
            {
                'company_name': faker.company(),
                'duration': f"Full-time · {random.randint(1, 5)} yrs",
                'designations': [
                    {
                        'designation': random.choice(['Data Scientist', 'Junior Analyst', 'Machine Learning Engineer']),
                        'duration': f"{faker.date_between(start_date='-5y', end_date='today')} · {random.randint(1, 3)} yrs",
                        'location': faker.city(),
                        'projects': [
                            {'title': 'Customer Behavior Modeling', 
                             'description': 'Developed predictive models for customer segmentation.'},
                            {'title': 'Big Data Pipeline Optimization', 
                             'description': 'Implemented scalable ETL pipelines using Apache Spark.'}
                        ]
                    }
                ]
            }
        ],
        'education': [
            {
                'college': 'Erasmus Mundus Joint Master Degree (EMJMD)',
                'degree': 'Master of Science (M.Sc.), Big Data Management and Analytics (BDMA)',
                'duration': f"{random.randint(2018, 2024)} - {random.randint(2019, 2025)}"
            },
            {
                'college': faker.company(),
                'degree': faker.job(),
                'duration': f"{random.randint(2000, 2018)} - {random.randint(2001, 2019)}"
            }
        ]
    }

profiles = [generate_profile() for _ in range(500)]

with open('data/profiles.json', 'w') as file:
    json.dump(profiles, file, indent=4)
