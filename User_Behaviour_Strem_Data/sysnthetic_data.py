import random
from datetime import datetime, timedelta
import time
import pytz
from faker import Faker

fake = Faker()

# Rule 1: A user ID should repeat a certain number of times
USER_REPETITION = 5
SESSION_REPETITION = 3

# Pre-generate user and session IDs
users = [f"U{random.randint(100000000, 999999999)}" for _ in range(10)]  # 10 unique users
sessions_per_user = {user: [f"S{random.randint(100000000, 999999999)}" for _ in range(SESSION_REPETITION)] for user in users}

def generate_scholarship_data():
    return {
        'Search Input': fake.domain_word() + " scholarship",
        'Scholarship ID': fake.uuid4()[:8].upper(),
        'Field of Study': fake.job(),
        'Country': fake.country()
    }

def generate_synthetic_data():
    base_timestamp = datetime(2025, 3, 8, 16, 42, 15, tzinfo=pytz.timezone('CET'))
    
    pages = ['/scholarships', '/details', '/faq']
    
    filter_parameters = [
        'Filter: {{Deadline: {}, Country: {}, Degree: {}, Field of Study: {}, Type: {}}}'
    ]

    user_index = 0
    session_index = 0
    user_repeat_count = 0
    session_repeat_count = 0

    while True:
        user_id = users[user_index]
        session_id = sessions_per_user[user_id][session_index]

        user_repeat_count += 1
        if user_repeat_count >= USER_REPETITION:
            user_repeat_count = 0
            user_index = (user_index + 1) % len(users)
            session_index = 0

        session_repeat_count += 1
        if session_repeat_count >= SESSION_REPETITION:
            session_repeat_count = 0
            session_index = (session_index + 1) % len(sessions_per_user[user_id])

        timestamp = base_timestamp + timedelta(seconds=random.randint(0, 86400))
        scholarship_data = generate_scholarship_data()

        page = random.choice(pages)
        
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
                deadline = random.choice(['Spring', 'Fall', 'None'])
                country = random.choice([scholarship_data['Country'], 'None'])
                degree = random.choice(['Bachelor', 'Master', 'PhD', 'None'])
                field = scholarship_data['Field of Study'] if random.choice([True, False]) else 'None'
                scholarship_type = random.choice(['Full', 'Partial', 'None'])
                
                clicked_parameter = filter_parameters[0].format(
                    deadline, country, degree, field, scholarship_type
                )

        data = {
            "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "User ID": user_id,
            "Session ID": session_id,
            "Page": page,
            "Clicked Element": clicked_element,
            "Clicked Parameter": clicked_parameter,
            "Duration": round(random.expovariate(1/0.8), 2),
            "Location": f"{fake.latitude()}, {fake.longitude()}",
        }
        
        yield data
        time.sleep(random.uniform(0.1, 1.0))
