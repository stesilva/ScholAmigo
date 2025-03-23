import random
from datetime import datetime, timedelta
from faker import Faker
import pytz

fake = Faker()

class ClickEventGenerator:
    def __init__(self):
        self.movie_titles = ["Inception", "The Dark Knight", "Interstellar", "Iron Man 1"]
        self.user_ids = [f"{random.randint(100000000, 999999999)}" for _ in range(10)]  # Limited users
        self.sessions = {}  # Store ongoing sessions for each user
        self.pages = ['/scholarships', '/details', '/faq']
        self.deadline = ['Spring', 'Fall', 'None']
        self.scholarshipType = ['Full', 'Partial', 'None']
        self.degreeType = ['Bachelor', 'Master', 'PhD', 'None']
        self.filter_parameters = [
            'Filter: {{Deadline: {}, Desired Country: {}, Country Origin: {}, Achieved Degree: {},  Desired Degree: {}, Field of Study: {}, Type: {}}}'
        ]
        self.base_timestamp =   datetime(2025, 3, 8, 16, 42, 15, tzinfo=pytz.timezone('CET'))

    def generate_synthetic_data(self):
        return {
            'Search Input': fake.domain_word() + " scholarship",
            'Scholarship ID': fake.uuid4()[:8].upper(),
            'Field of Study': fake.job(),
            'Country': fake.country(),
            'Country Origin': fake.country()
        }

    def generate_click_event(self):
        user_id = random.choice(self.user_ids)
        session_id = f"{random.randint(1, 1000)}"
           
        timestamp = self.base_timestamp + timedelta(seconds=random.randint(0, 86400))
        scholarship_data = self.generate_synthetic_data()

        page = random.choice(self.pages)
            
        if page == '/faq':
            clicked_element = None
            clicked_parameter = None
            
        elif page == '/details':
            clicked_element = random.choice(['"Save" button', '"Scholarship Details" link'])
            clicked_parameter = f'Scholarship ID: {scholarship_data["Scholarship ID"]}'
            
        elif page == '/scholarships':
            clicked_element = random.choice(['"Search" button','"Filter" dropdown'])

            if clicked_element == '"Search" button':
                clicked_parameter = f'Search Input: {scholarship_data["Search Input"]}'
            
            elif clicked_element == '"Filter" dropdown':
                deadline = random.choice(self.deadline)
                desiredCountry = random.choice([scholarship_data['Country'], 'None'])
                countryOrigin = random.choice([scholarship_data['Country Origin'], 'None'])
                achievedDegree = random.choice(self.degreeType)
                desiredDegree = random.choice(self.degreeType)
                field = scholarship_data['Field of Study'] if random.choice([True, False]) else 'None'
                scholarship_type = random.choice(self.scholarshipType)
                    
                clicked_parameter = self.filter_parameters[0].format(
                    deadline, desiredCountry, countryOrigin, achievedDegree, desiredDegree, field, scholarship_type
                )

        event = {
                "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S %Z"),
                "User_ID": user_id,
                "Session_ID": session_id,
                "Page": page,
                "Clicked_Element": clicked_element,
                "Clicked_Parameter": clicked_parameter,
                "Duration": round(random.expovariate(1/0.8), 2),
                "Location": f"{fake.latitude()}, {fake.longitude()}",
            }
            
        return event