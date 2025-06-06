import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
from typing import Dict, Any, List, Optional
import os


fake = Faker()

# Use /tmp directory for testing
DEFAULT_DATA_DIR = '/tmp/scholarship_data/scholarship'

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
    def __init__(self, scholarship_data_path=None):
        self.user_ids = [f"{random.randint(100000000, 999999999)}" for _ in range(10)]
        self.pages = ['/scholarships', '/details', '/faq']
        self.intake = ['spring', 'fall', 'none']
        self.scholarshipType = ['fully funded', 'partially funded', 'none']
        self.scholarshipStatus = ['open', 'closed', 'none']
        self.programLevel = ['bachelor', 'master', 'phd', 'postdoctoral researchers', 'faculty', 'none']
        self.requiredLevel = ['high school diploma', 'bachelor', 'master', 'phd', 'postdoctoral researchers', 'faculty', 'none']
        self.fieldStudy = [field.lower() for field in detailed_fields]
        self.filter_parameters = [
            'Filter: {{Intake: {}, Desired Country: {}, Country Origin: {}, Achieved Degree: {},  Desired Degree: {}, Field of Study: {}, Type: {}, Status:{}}}'
        ]
        
        # For testing, just use /tmp directory which should be writable
        self.data_path = scholarship_data_path or \
                        os.getenv('SCHOLARSHIP_DATA_PATH') or \
                        os.path.join(DEFAULT_DATA_DIR, 'scholarships_processed_new.parquet')
        
        # For testing purposes, if no data file exists, we'll just use synthetic data
        if not os.path.exists(self.data_path):
            print(f"No data file found at {self.data_path}, using synthetic data generation")
            self.scholarship_data = []
        else:
            # Load scholarship data
            self.scholarship_data = self._load_scholarship_data(self.data_path)

    def _load_scholarship_data(self, data_path: str) -> List[Dict[str, Any]]:
        """Load scholarship data from parquet file"""
        try:
            if not os.path.exists(data_path):
                raise FileNotFoundError(f"Data file not found at {data_path}")
            
            print(f"Loading scholarship data from: {data_path}")
            # Read the parquet file using pandas
            df = pd.read_parquet(data_path)
            
            # Convert DataFrame to list of dictionaries
            scholarships = df[[
                'scholarship_id',
                'scholarship_name',
                'program_country',
                'origin_country',
                'program_level',
                'required_level',
                'fields_of_study_code',
                'funding_category',
                'status',
                'intake'
            ]].to_dict('records')
            
            print(f"Successfully loaded {len(scholarships)} scholarships")
            
            # Convert to our format
            return [
                {
                    'Scholarship ID': row['scholarship_id'],
                    'Scholarship Name': row['scholarship_name'],
                    'Country': row['program_country'],
                    'Country Origin': row['origin_country'],
                    'Program Level': row['program_level'],
                    'Required Level': row['required_level'],
                    'Field of Study': row['fields_of_study_code'],
                    'Type': row['funding_category'],
                    'Status': row['status'],
                    'Intake': row['intake']
                }
                for row in scholarships
            ]
        except Exception as e:
            print(f"Warning: Could not load scholarship data: {str(e)}")
            print("Falling back to synthetic data generation")
            return []

    def _get_single_value(self, field_value: Any) -> str:
        """Extract a single value from a field that might be a list or numpy array"""
        if field_value is None:
            return 'none'
            
        # Handle numpy arrays or pandas series
        if hasattr(field_value, 'tolist'):
            field_value = field_value.tolist()
            
        # Handle lists
        if isinstance(field_value, list):
            if not field_value:  # Empty list
                return 'none'
            return str(random.choice(field_value)).lower()
            
        # Handle everything else
        return str(field_value).lower()

    def _generate_synthetic_data(self) -> Dict[str, Any]:
        """Generate data using real scholarship data if available, otherwise synthetic"""
        if self.scholarship_data:
            # Use real scholarship data
            scholarship = random.choice(self.scholarship_data)
            return scholarship
        else:
            # Fallback to synthetic data
            return {
                'Scholarship ID': fake.uuid4()[:8].upper(),
                'Scholarship Name': fake.company() + " Scholarship",
                'Country': fake.country(),
                'Country Origin': [fake.country()],
                'Program Level': random.choice(self.programLevel),
                'Required Level': random.choice(self.requiredLevel),
                'Field of Study': [random.choice(self.fieldStudy)],
                'Type': random.choice(self.scholarshipType),
                'Status': random.choice(self.scholarshipStatus),
                'Intake': random.choice(self.intake)
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
        
        # Get possible search terms and ensure they're properly handled
        possible_terms = [
            scholarship_data.get('Scholarship Name', ''),
            self._get_single_value(scholarship_data.get('Field of Study')),
            self._get_single_value(scholarship_data.get('Country')),
            self._get_single_value(scholarship_data.get('Country Origin'))
        ]
        
        # Filter out empty or None values and select a random term
        valid_terms = [term for term in possible_terms if term and term.lower() != 'none']
        search_term = random.choice(valid_terms) if valid_terms else 'scholarship'
        
        event.update({
            "Clicked_Element": 'Search button',
            "Clicked_Parameter": f'Search Input: {search_term}'
        })
        return event

    def generate_filter_event(self):
        event = self._generate_base_event('/scholarships')
        scholarship_data = self._generate_synthetic_data()
        
        # Get single values for list fields or use existing single values
        intake = self._get_single_value(scholarship_data.get('Intake'))
        desiredCountry = self._get_single_value(scholarship_data.get('Country'))
        countryOrigin = self._get_single_value(scholarship_data.get('Country Origin'))
        achievedDegree = self._get_single_value(scholarship_data.get('Required Level'))
        desiredDegree = self._get_single_value(scholarship_data.get('Program Level'))
        field = self._get_single_value(scholarship_data.get('Field of Study'))
        scholarship_type = self._get_single_value(scholarship_data.get('Type'))
        scholarship_status = self._get_single_value(scholarship_data.get('Status'))
        
        clicked_parameter = self.filter_parameters[0].format(
            intake, desiredCountry, countryOrigin, achievedDegree, 
            desiredDegree, field, scholarship_type, scholarship_status
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