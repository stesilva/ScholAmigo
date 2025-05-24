import json
import random
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

#Load JSON base file
with open('outputs/linkedin/2025-05-02_16-02_linkedin_profile_data.json', encoding='utf-8') as f:
    profiles = json.load(f)

if isinstance(profiles, dict):
    profiles = list(profiles.values())

#Shuffle linkedin profiles and split between 10% alumni and 90% users
random.shuffle(profiles)
split_idx = int(0.9 * len(profiles))
user_profiles = profiles[:split_idx]
alumni_profiles = profiles[split_idx:]

#possible account plan for user
plans = ['Free', 'Premium']
#possible status for alumni
alumni_status = ['available', 'not available']
#synthetic scholarships for alumni
scholarships = [
    'International Leaders Scholarship',
    'STEM Excellence Award',
    'Global Innovators Grant',
    'Women in Tech Fellowship',
    'Future Entrepreneurs Scholarship'
]

def build_user_profile(profile):
    return {
        "Name": profile.get("name", fake.name()),
        "Age": profile.get("age", random.randint(18, 70)),
        "Gender": fake.random_element(['Male', 'Female', 'Other']),
        "Email": profile.get("email", fake.email()),
        "Country": profile.get("country", fake.country()),
        "Plan": random.choice(plans)
    }

def build_alumni_profile(profile):
    return {
        "Name": profile.get("name", fake.name()),
        "Age": profile.get("age", random.randint(22, 75)),
        "Gender": fake.random_element(['Male', 'Female', 'Other']),
        "Email": profile.get("email", fake.email()),
        "Country": profile.get("country", fake.country()),
        "Associeted_Scholarship": random.choice(scholarships),
        "Year_Scholarship": random.randint(2000, 2025),
        "Status": random.choice(alumni_status)
    }

#build basic profile
user_basic_profiles = [build_user_profile(p) for p in user_profiles]
alumni_basic_profiles = [build_alumni_profile(p) for p in alumni_profiles]

#save to JSON
with open('outputs/users/user_basic_profiles.json', 'w', encoding='utf-8') as f:
    json.dump(user_basic_profiles, f, ensure_ascii=False, indent=2)

with open('outputs/users/alumni_basic_profiles.json', 'w', encoding='utf-8') as f:
    json.dump(alumni_basic_profiles, f, ensure_ascii=False, indent=2)

