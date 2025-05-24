import json
import random
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# Carregar o arquivo JSON
with open('outputs/linkedin/2025-05-02_16-02_linkedin_profile_data.json', encoding='utf-8') as f:
    profiles = json.load(f)

# Se o arquivo for uma lista de perfis, ok. Se for um dicionário, adapte conforme necessário.
# Para garantir, vamos tentar extrair os perfis de uma lista:
if isinstance(profiles, dict):
    # Se o JSON for um dicionário, tente extrair os perfis de uma chave conhecida
    profiles = list(profiles.values())

# Embaralhar e dividir em 90% e 10%
random.shuffle(profiles)
split_idx = int(0.9 * len(profiles))
user_profiles = profiles[:split_idx]
alumni_profiles = profiles[split_idx:]

# Planos possíveis para usuários
plans = ['Free', 'Premium']
# Status possíveis para alumni
alumni_status = ['available', 'not available']
# Bolsas fictícias para alumni
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

# Gerar perfis para os arquivos
user_basic_profiles = [build_user_profile(p) for p in user_profiles]
alumni_basic_profiles = [build_alumni_profile(p) for p in alumni_profiles]

# Salvar em arquivos JSON
with open('outputs/users/user_basic_profiles.json', 'w', encoding='utf-8') as f:
    json.dump(user_basic_profiles, f, ensure_ascii=False, indent=2)

with open('outputs/users/alumni_basic_profiles.json', 'w', encoding='utf-8') as f:
    json.dump(alumni_basic_profiles, f, ensure_ascii=False, indent=2)

