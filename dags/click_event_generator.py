import random
from datetime import datetime, timedelta
from faker import Faker
import pytz

fake = Faker()

class ClickEventGenerator:
    def __init__(self):
        self.user_ids = [f"{random.randint(100000000, 999999999)}" for _ in range(10)]
        self.pages = ['/scholarships', '/details', '/faq']
        self.deadline = ['Spring', 'Fall', 'None']
        self.scholarshipType = ['Full', 'Partial', 'None']
        self.degreeType = ['Bachelor', 'Master', 'PhD', 'None']
        self.filter_parameters = [
            'Filter: {{Deadline: {}, Desired Country: {}, Country Origin: {}, Achieved Degree: {},  Desired Degree: {}, Field of Study: {}, Type: {}}}'
        ]
        self.base_timestamp = datetime(2025, 3, 8, 16, 42, 15, tzinfo=pytz.timezone('CET'))

    def _generate_synthetic_data(self):
        return {
            'Search Input': fake.domain_word() + " scholarship",
            'Scholarship ID': fake.uuid4()[:8].upper(),
            'Field of Study': fake.job(),
            'Country': fake.country(),
            'Country Origin': fake.country()
        }

    def _generate_base_event(self, page):
        user_id = random.choice(self.user_ids)
        session_id = f"{random.randint(1, 1000)}"
        timestamp = self.base_timestamp + timedelta(seconds=random.randint(0, 86400))
        
        return {
            "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "User_ID": user_id,
            "Session_ID": session_id,
            "Page": page,
            "Duration": round(random.expovariate(1/0.8), 2),
            "Location": f"{fake.latitude()}, {fake.longitude()}",
        }

    def generate_search_event(self):
        event = self._generate_base_event('/scholarships')
        scholarship_data = self._generate_synthetic_data()
        event.update({
            "Clicked_Element": '"Search" button',
            "Clicked_Parameter": f'Search Input: {scholarship_data["Search Input"]}'
        })
        return event

    def generate_filter_event(self):
        event = self._generate_base_event('/scholarships')
        scholarship_data = self._generate_synthetic_data()
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
        event.update({
            "Clicked_Element": '"Filter" dropdown',
            "Clicked_Parameter": clicked_parameter
        })
        return event

    def generate_details_event(self):
        event = self._generate_base_event('/details')
        scholarship_data = self._generate_synthetic_data()
        event.update({
            "Clicked_Element": random.choice(['"Save" button', '"Scholarship Details" link']),
            "Clicked_Parameter": f'Scholarship ID: {scholarship_data["Scholarship ID"]}'
        })
        return event

    def generate_faq_event(self):
        event = self._generate_base_event('/faq')
        event.update({
            "Clicked_Element": None,
            "Clicked_Parameter": None
        })
        return event

    def generate_random_event(self):
        event_type = random.choice(['search', 'filter', 'details', 'faq'])
        if event_type == 'search':
            return self.generate_search_event()
        elif event_type == 'filter':
            return self.generate_filter_event()
        elif event_type == 'details':
            return self.generate_details_event()
        else:
            return self.generate_faq_event()
