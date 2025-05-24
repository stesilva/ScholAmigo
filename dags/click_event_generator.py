import random
from datetime import datetime, timedelta
from faker import Faker


fake = Faker()


detailed_fields = [
    "Basic programmes and qualifications",
    "Literacy and numeracy",
    "Personal skills and development",
    "Education science",
    "Training for pre-school teachers",
    "Teacher training without subject specialisation",
    "Teacher training with subject specialisation",
    "Audio-visual techniques and media production",
    "Fashion, interior and industrial design",
    "Fine arts",
    "Handicrafts",
    "Music and performing arts",
    "Religion and theology",
    "History and archaeology",
    "Philosophy and ethics",
    "Language acquisition",
    "Literature and linguistics",
    "Economics",
    "Political sciences and civics",
    "Psychology",
    "Sociology and cultural studies",
    "Journalism and reporting",
    "Library, information and archival studies",
    "Accounting and taxation",
    "Finance, banking and insurance",
    "Management and administration",
    "Marketing and advertising",
    "Secretarial and office work",
    "Wholesale and retail sales",
    "Work skills",
    "Law",
    "Biology",
    "Biochemistry",
    "Environmental sciences",
    "Natural environments and wildlife",
    "Chemistry",
    "Earth sciences",
    "Physics",
    "Mathematics",
    "Statistics",
    "Computer use",
    "Database and network design and administration",
    "Software and applications development and analysis",
    "Chemical engineering and processes",
    "Environmental protection technology",
    "Electricity and energy",
    "Electronics and automation",
    "Mechanics and metal trades",
    "Motor vehicles, ships and aircraft",
    "Food processing",
    "Materials (glass, paper, plastic and wood)",
    "Textiles (clothes, footwear and leather)",
    "Mining and extraction",
    "Architecture and town planning",
    "Building and civil engineering",
    "Crop and livestock production",
    "Horticulture",
    "Forestry",
    "Fisheries",
    "Veterinary",
    "Dental studies",
    "Medicine",
    "Nursing and midwifery",
    "Medical diagnostic and treatment technology",
    "Therapy and rehabilitation",
    "Pharmacy",
    "Traditional and complementary medicine and therapy",
    "Care of the elderly and of disabled adults",
    "Child care and youth services",
    "Social work and counselling",
    "Domestic services",
    "Hair and beauty services",
    "Hotel, restaurants and catering",
    "Sports",
    "Travel, tourism and leisure",
    "Community sanitation",
    "Occupational health and safety",
    "Military and defence",
    "Protection of persons and property",
    "Transport services",
    "None"
]

#generates synthetic clickstream events for various actions types on the website
class ClickEventGenerator:
    def __init__(self):
        self.user_ids = [f"{random.randint(100000000, 999999999)}" for _ in range(10)]
        self.pages = ['/scholarships', '/details', '/faq']
        self.deadline = ['Spring', 'Fall', 'None']
        self.scholarshipType = ['Fully Funded', 'Partially Funded', 'None']
        self.scholarshipStatus = ['Open', 'Closed', 'None']
        self.programLevel = ['Bachelor', 'Master', 'PhD', 'Postdoctoral researchers', 'Faculty' 'None']
        self.requiredLevel = ['High School Diploma','Bachelor', 'Master', 'PhD', 'Postdoctoral researchers', 'Faculty' 'None']
        self.fieldStudy = detailed_fields
        self.filter_parameters = [
            'Filter: {{Deadline: {}, Desired Country: {}, Country Origin: {}, Achieved Degree: {},  Desired Degree: {}, Field of Study: {}, Type: {}, Status:{}}}'
        ]

    def _generate_synthetic_data(self):
        return {
            'Search Input': fake.domain_word() + " scholarship",
            'Scholarship ID': fake.uuid4()[:8].upper(),
            'Country': fake.country(),
            'Country Origin': fake.country()
        }

    def _generate_base_event(self, page):
        user_id = random.choice(self.user_ids)
        session_id = f"{random.randint(1, 1000)}"
        now = datetime.now()
        random_seconds_ago = random.randint(0, 3600)
        timestamp = now - timedelta(seconds=random_seconds_ago)
        
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
            "Clicked_Element": 'Search button',
            "Clicked_Parameter": f'Search Input: {scholarship_data["Search Input"]}'
        })
        return event

    def generate_filter_event(self):
        event = self._generate_base_event('/scholarships')
        scholarship_data = self._generate_synthetic_data()
        deadline = random.choice(self.deadline)
        desiredCountry = random.choice([scholarship_data['Country'], 'None'])
        countryOrigin = random.choice([scholarship_data['Country Origin'], 'None'])
        achievedDegree = random.choice(self.requiredLevel)
        desiredDegree = random.choice(self.programLevel)
        field = random.choice(self.fieldStudy)
        scholarship_type = random.choice(self.scholarshipType)
        scholarship_status = random.choice(self.scholarshipStatus)
        
        clicked_parameter = self.filter_parameters[0].format(
            deadline, desiredCountry, countryOrigin, achievedDegree, desiredDegree, field, scholarship_type, scholarship_status
        )
        event.update({
            "Clicked_Element": 'Filter dropdown',
            "Clicked_Parameter": clicked_parameter
        })
        return event

    def generate_details_event(self):
        event = self._generate_base_event('/details')
        scholarship_data = self._generate_synthetic_data()
        event.update({
            "Clicked_Element": random.choice(['Save button', 'Scholarship Details link']),
            "Clicked_Parameter": f'Scholarship ID: {scholarship_data["Scholarship ID"]}'
        })
        return event

    def generate_faq_event(self):
        event = self._generate_base_event('/faq')
        event.update({
            "Clicked_Element": 'FAQ link',
            "Clicked_Parameter": 'FAQ Page'
        })
        return event

    def generate_random_event(self):
        event_type = random.choice(['search', 'filter', 'details', 'faq'])
        #Search events: Simulates a user clicking the "Search" button with input data
        if event_type == 'search':
            return self.generate_search_event()
        #Filter events: Simulates a user applying filters with various parameters
        elif event_type == 'filter':
            return self.generate_filter_event()
        #Details events: Simulates interactions with scholarship details or saving actions
        elif event_type == 'details':
            return self.generate_details_event()
        else:
            #FAQ events: Simulates interactions with the FAQ page
            return self.generate_faq_event()


if __name__ == "__main__":
    generator = ClickEventGenerator()
    for _ in range(10):
        event = generator.generate_random_event()
        print(event)