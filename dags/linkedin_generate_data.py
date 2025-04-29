import warnings
from dotenv import load_dotenv
import boto3
from datetime import datetime
from send_data_to_aws import send_data_to_aws
import random
from faker import Faker

warnings.filterwarnings("ignore")
load_dotenv()

certifications = [
    "CompTIA A+",
    "CompTIA Network+",
    "CompTIA Security+",
    "Cisco Certified Network Associate (CCNA)",
    "AWS Certified Solutions Architect",
    "Microsoft Certified: Azure Fundamentals",
    "Certified Information Systems Security Professional (CISSP)",
    "Google Professional Cloud Architect",
    "Certified Ethical Hacker (CEH)",
    "Project Management Professional (PMP)",
    "Certified ScrumMaster (CSM)",
    "Lean Six Sigma Green Belt",
    "Certified Business Analysis Professional (CBAP)",
    "Agile Certified Practitioner (PMI-ACP)",
    "Professional in Human Resources (PHR)",
    "Senior Professional in Human Resources (SPHR)",
    "Society for Human Resource Management - Certified Professional (SHRM-CP)",
    "Certified Nursing Assistant (CNA)",
    "Basic Life Support (BLS)",
    "Registered Health Information Technician (RHIT)",
    "Certified Medical Assistant (CMA)",
    "Google Ads Certification",
    "HubSpot Content Marketing Certification",
    "Adobe Certified Professional",
    "Facebook Certified Digital Marketing Associate",
    "OSHA Safety Certification",
    "Certified Electrician License",
    "HVAC Certification",
    "Welding Certification (AWS Certified Welder)",
    "LEED Green Associate",
    "Certified Energy Manager (CEM)"
]


skills = [
    "Communication",
    "Problem-Solving",
    "Teamwork",
    "Critical Thinking",
    "Time Management",
    "Adaptability",
    "Leadership",
    "Creativity",
    "Technical Writing",
    "Data Analysis",
    "Project Management",
    "Programming",
    "Cybersecurity",
    "Networking",
    "Cloud Computing",
    "Digital Marketing",
    "Graphic Design",
    "Sales",
    "Customer Service",
    "Negotiation",
    "Financial Analysis",
    "Research",
    "Public Speaking",
    "Emotional Intelligence",
    "Python",
    "SQL",
    "Java",
    "JavaScript",
    "C++",
    "HTML",
    "CSS",
    "R",
    "MATLAB",
    "Excel",
    "PowerPoint",
    "Word",
    "Photoshop",
    "Illustrator",
    "AutoCAD"]

companies = [
    "Apple",
    "Microsoft",
    "Amazon",
    "Google",
    "Meta",
    "Tesla",
    "IBM",
    "Intel",
    "Netflix",
    "Adobe",
    "Oracle",
    "Salesforce",
    "Nvidia",
    "Samsung",
    "Sony",
    "Cisco",
    "Uber",
    "Airbnb",
    "Spotify",
    "Zoom",
    "Shopify",
    "Twitter",
    "LinkedIn",
    "SpaceX",
    "Snapchat",
    "Dell Technologies",
    "HP",
    "TikTok",
    "Qualcomm",
    "AT&T",
    "Verizon",
    "Alibaba",
    "Baidu",
    "Tencent",
    "ByteDance",
    "PayPal",
    "Square",
    "Stripe",
    "Goldman Sachs",
    "JPMorgan Chase",
    "Bank of America",
    "Wells Fargo",
    "Citigroup",
    "Morgan Stanley",
    "Visa",
    "Mastercard",
    "American Express",
    "Boeing",
    "Lockheed Martin",
    "Northrop Grumman",
    "General Electric",
    "Ford",
    "General Motors",
    "Toyota",
    "Honda",
    "Volkswagen",
    "BMW",
    "Mercedes-Benz",
    "Pfizer",
    "Moderna",
    "Johnson & Johnson",
    "Roche",
    "Novartis",
    "Merck",
    "GlaxoSmithKline",
    "Unilever",
    "Procter & Gamble",
    "Coca-Cola",
    "PepsiCo",
    "Nestle",
    "Walmart",
    "Costco",
    "Target",
    "Home Depot",
    "Lowe's",
    "Nike",
    "Adidas",
    "Starbucks",
    "McDonald's",
    "Burger King",
    "Domino's Pizza",
    "KFC",
    "LVMH",
    "Gucci",
    "Chanel",
    "Prada",
    "Zara",
    "H&M",
    "Chevron",
    "ExxonMobil",
    "Shell",
    "BP",
    "TotalEnergies",
    "Siemens",
    "ABB",
    "Accenture",
    "Capgemini",
    "Deloitte",
    "PwC",
    "EY",
    "KPMG"
]


languages = [
    "English",
    "Mandarin/Chinese",
    "Hindi",
    "Spanish",
    "Arabic",
    "French",
    "Portuguese",
    "Russian",
    "Japanese",
    "German",
    "Malay/Indonesian",
    "Telugu"
]

courses = [
    "Introduction to Computer Science",
    "Data Structures and Algorithms",
    "Web Development",
    "Mobile App Development",
    "Machine Learning",
    "Artificial Intelligence",
    "Cloud Computing",
    "Cybersecurity Fundamentals",
    "Project Management Basics",
    "Digital Marketing",
    "Graphic Design Principles",
    "Financial Accounting",
    "Entrepreneurship",
    "Business Analytics",
    "Public Speaking",
    "Creative Writing",
    "Psychology 101",
    "Introduction to Philosophy",
    "Environmental Science",
    "Biology Basics",
    "Chemistry Fundamentals",
    "Physics for Beginners",
    "Statistics and Probability",
    "Economics Principles",
    "Human Resource Management",
    "Supply Chain Management",
    "International Relations",
    "Political"]

honors = [
    "Summa Cum Laude",
    "Magna Cum Laude",
    "Cum Laude",
    "Dean's List",
    "Honor Roll",
    "National Merit Scholar",
    "Valedictorian",
    "Salutatorian",
    "Phi Beta Kappa",
    "Golden Key International Honour Society",
    "Presidential Scholar",
    "Chancellor Award",
    "Outstanding Student Award",
    "Academic Excellence Award",
    "Best Research Paper Award",
    "Employee of the Month",
    "Top Performer Award",
    "Leadership Excellence Award",
    "Innovation Award",
    "Community Service Award"
]

country = [
    "United States",
    "Canada",
    "Mexico",
    "Brazil",
    "Argentina",
    "United Kingdom",
    "France",
    "Germany",
    "Italy",
    "Spain",
    "Portugal",
    "Netherlands",
    "Belgium",
    "Switzerland",
    "Sweden",
    "Norway",
    "Denmark",
    "Finland",
    "Russia",
    "China",
    "Japan",
    "South Korea",
    "India",
    "Pakistan",
    "Bangladesh",
    "Indonesia",
    "Vietnam",
    "Thailand",
    "Malaysia",
    "Philippines",
    "Australia",
    "New Zealand",
    "South Africa",
    "Egypt",
    "Nigeria",
    "Kenya",
    "Ukraine"]


majors = [
    "Computer Science", "Business Administration", "Engineering", "Psychology", 
    "Biology", "Mathematics", "Economics", "Political Science", "History", 
    "Sociology", "Chemistry", "Physics", "Environmental Science", "Nursing", 
    "Education", "Art", "Music", "Theater", "Philosophy"
]

class LinkedInGenerateData:
    def __init__(self):
        self.profile_data = {}
        self.faker = Faker()
        self.faker.seed_instance(0)  #set a seed for reproducibility
    def generate_profile(self, url):
        self.profile_data = {}
        self.profile_data['url'] = url
        self.generate_basic_info()
        self.generate_section('experience', self.generate_experiences)
        self.generate_section('education', self.generate_educations)
        self.generate_section('certifications', self.generate_licenses)
        self.generate_section('skills', self.generate_skills)
        self.generate_section('languages', self.generate_languages)
        self.generate_section('courses', self.generate_courses)
        self.generate_section('honors', self.generate_honors)
        return self.profile_data

    def generate_section(self, section_name, generate_function):
        try:
            print(f"Generating {section_name}...")
            generate_function()
        except Exception as e:
            print(f"Error generating {section_name}: {str(e)}")

    #Synthetic data generation for basic profile information
    def generate_basic_info(self):
        try:
            self.profile_data['name'] = self.faker.name()
            self.profile_data['age'] = self.faker.random_int(min=18,max=100)
            self.profile_data['email'] = self.faker.email()
            self.profile_data['country'] = random.choice(country)
        except Exception as e:
            print(f"Error generating basic info: {str(e)}")

    #Synthetic data generation for experiences
    def generate_experiences(self):
        try:
            self.profile_data['experiences'] = [
                {
                    'company_name': f"{random.choice(companies)}",
                    'designations': [
                        {
                            'designation': f"{self.faker.job()}",
                            'duration': f"{random.randint(1, 5)} years",
                        }
                    ]
                }
                for _ in range(random.randint(1, 5))
            ]
        except Exception as e:
            print(f"Error generating experiences: {str(e)}")

    #Synthetic data generation for educations
    def generate_educations(self):
        try:
            self.profile_data['educations'] = [
                {
                    'college': f"University {self.faker.street_name()}",
                    'degree': random.choice(['High School','Bachelor', 'Master', 'PhD']),
                    'graduation_year': f"{random.randint(2010, 2025)}",
                    'major': f"{random.choice(majors)}",
                }
                for _ in range(random.randint(1, 3))
            ]
        except Exception as e:
            print(f"Error generating educations: {str(e)}")

    #Synthetic data generation for licenses
    def generate_licenses(self):
        try:
            self.profile_data['licenses'] = [
                {
                    'name': f"{random.choice(certifications)}",
                    'institute': f"Institute {self.faker.company()}",
                    'issued_date': f"{random.randint(2010, 2025)}"
                }
                for _ in range(random.randint(1, 3))
            ]
        except Exception as e:
            print(f"Error generating licenses: {str(e)}")

    #Synthetic data generation for skills
    def generate_skills(self):
        try:
            self.profile_data['skills'] = [
                {'name': f"{random.choice(skills)}"}
                for _ in range(random.randint(3, 10))
            ]
        except Exception as e:
            print(f"Error generating skills: {str(e)}")

    #Synthetic data generation for languages
    def generate_languages(self):
        try:
            self.profile_data['languages'] = [
                {
                    'name': f"{random.choice(languages)}",
                    'level': random.choice(['Beginner', 'Intermediate', 'Advanced'])
                }
                for _ in range(random.randint(1, 5))
            ]
        except Exception as e:
            print(f"Error generating languages: {str(e)}")

    #Synthetic data generation for courses
    def generate_courses(self):
        try:
            self.profile_data['courses'] = [
                {
                    'course_name': f"{random.choice(courses)}",
                    'associated_with': f"Organization {self.faker.company()}",
                }
                for _ in range(random.randint(1, 5))
            ]
        except Exception as e:
            print(f"Error generating courses: {str(e)}")

    #Synthetic data generation for honors
    def generate_honors(self):
        try:
            self.profile_data['honors'] = [
                {'honor_name': f"{random.choice(honors)}"}
                for _ in range(random.randint(1, 3))
            ]
        except Exception as e:
            print(f"Error generating honors: {str(e)}")
        
    #save the generated data to S3 bucket
    def save_data(self, bucket_name, all_profile_data, folder_name):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        s3_file_name = f"{folder_name}/{timestamp}_linkedin_profile_data.json"
        send_data_to_aws(all_profile_data, bucket_name, s3_file_name)
       

#retrieve the list of user to perform the generating
def retrive_linkedin_urls_s3(s3,bucket_name,folder_name):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name) 
    
    if 'Contents' not in response:
        print(f"Error: No file found at {bucket_name}")
        return None
    
    latest_file = max(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_file_key = latest_file['Key']
    
    file_response = s3.get_object(Bucket=bucket_name, Key=latest_file_key)
    content = file_response['Body'].read().decode('utf-8')
    
    return content

def generate_linkedin_profiles(generater, linkedin_urls, bucket_name, folder_name):
    all_profile_data = [] 

    lines = linkedin_urls.strip().split("\n")
    lines = [line.replace('\r', '') for line in lines]
    headers = lines[0].split(",")
    rows = [dict(zip(headers, line.split(","))) for line in lines[1:]]

    try:
        for row in rows: #for each retrieved profile
            profile_title = row['name']
            profile_url = row['url']
            print(f"Generating profile: {profile_title} - {profile_url}")

            try:
                profile_data = generater.generate_profile(profile_url)
                if profile_data:
                    all_profile_data.append(profile_data)
                    print(f"Successfully generated data for: {profile_title}")
                else:
                    print(f"Warning: No data generated for {profile_title} - {profile_url}")

            except Exception as e:
                print(f"Error generating {profile_title} - {profile_url}: {e}")

    except FileNotFoundError:
        print(f"Error: No file found at {bucket_name}")
        return
    except Exception as e:
        print(f"An error occurred while reading the data: {e}")
        return
    

    try:
        generater.save_data(bucket_name,all_profile_data, folder_name)
        print(f"generated data saved to {bucket_name} - {folder_name}")
    except Exception as e:
        print(f"Error saving data to S3: {e}")        


def generate_linkedin():
    #retieve the list of users to generate from S3 bucket
    session = boto3.Session(profile_name="bdm_group_member")
    s3 = session.client("s3")
    bucket_name = 'linkedin-data-bdm'
    input_folder_name = 'application_data/'
    output_folder_name = 'linkedin_users_data/'
    linkedin_urls = retrive_linkedin_urls_s3(s3,bucket_name,input_folder_name)

    #generate synthetic data for the users
    generater = LinkedInGenerateData()
    generate_linkedin_profiles(generater,linkedin_urls,bucket_name,output_folder_name)

if __name__ == "__main__":
    generate_linkedin()